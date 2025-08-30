import os
import re
import json
import logging
from datetime import datetime, timezone, timedelta
from typing import List, Tuple, Dict, Optional
from collections import defaultdict

import discord
from discord.ext import commands
import aiosqlite
from dotenv import load_dotenv

# -------------------------
# Config & Logging
# -------------------------
load_dotenv()

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
log = logging.getLogger("term-bot")

DB_PATH = os.getenv("DB_PATH", "termbot.sqlite3")
COMMAND_PREFIX = os.getenv("PREFIX", "!")

# Power users (comma-separated Discord user IDs)
POWER_USER_IDS = {
    int(x) for x in os.getenv("POWER_USER_IDS", "").replace(" ", "").split(",") if x.strip().isdigit()
}

JSON_PATH = os.getenv("JSON_PATH", "bot_data.json")

intents = discord.Intents.default()
intents.message_content = True
intents.messages = True
intents.guilds = True

# -------------------------
# Database schema
# -------------------------
SCHEMA = '''
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS terms (
    guild_id INTEGER NOT NULL,
    term TEXT NOT NULL,
    PRIMARY KEY (guild_id, term)
);
CREATE TABLE IF NOT EXISTS term_meta (
    guild_id INTEGER NOT NULL,
    term TEXT NOT NULL,
    total_count INTEGER NOT NULL DEFAULT 0,
    last_mentioned TEXT,
    last_user TEXT,
    PRIMARY KEY (guild_id, term)
);
CREATE TABLE IF NOT EXISTS hits (
    guild_id INTEGER NOT NULL,
    term TEXT NOT NULL,
    user_id INTEGER NOT NULL,
    user_name TEXT NOT NULL,
    count INTEGER NOT NULL DEFAULT 0,
    last_seen TEXT,
    PRIMARY KEY (guild_id, term, user_id)
);
CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    guild_id INTEGER NOT NULL,
    channel_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    user_name TEXT NOT NULL,
    message_id INTEGER NOT NULL,
    term TEXT NOT NULL,
    content TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS moderation_actions (
    guild_id INTEGER NOT NULL,
    phrase TEXT NOT NULL,
    action_type TEXT NOT NULL CHECK(action_type IN ('delete', 'timeout', 'response')),
    action_value TEXT, -- timeout duration in minutes, or response text
    PRIMARY KEY (guild_id, phrase)
);
CREATE TABLE IF NOT EXISTS ignored_channels (
    guild_id INTEGER NOT NULL,
    channel_id INTEGER NOT NULL,
    PRIMARY KEY (guild_id, channel_id)
);
CREATE TABLE IF NOT EXISTS guild_settings (
    guild_id INTEGER NOT NULL PRIMARY KEY,
    ignore_commands BOOLEAN DEFAULT true,
    case_sensitive BOOLEAN DEFAULT false,
    min_word_length INTEGER DEFAULT 1,
    cooldown_seconds INTEGER DEFAULT 0,
    timeout_duration INTEGER DEFAULT 10
);
CREATE TABLE IF NOT EXISTS user_cooldowns (
    guild_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    term TEXT NOT NULL,
    last_increment TEXT,
    PRIMARY KEY (guild_id, user_id, term)
);
'''

async def init_db(db: aiosqlite.Connection):
    for stmt in SCHEMA.strip().split(";"):
        s = stmt.strip()
        if s:
            await db.execute(s)
    await db.commit()

# -------------------------
# Utilities
# -------------------------
def normalize_term(term: str) -> str:
    return term.strip().lower()

def build_patterns(terms: List[str], case_sensitive: bool = False) -> List[Tuple[str, re.Pattern]]:
    flags = 0 if case_sensitive else re.IGNORECASE
    patterns = []
    for term in terms:
        pattern = r'\b' + re.escape(term) + r'\b'
        patterns.append((term, re.compile(pattern, flags)))
    return patterns

def is_command_message(content: str, prefix: str) -> bool:
    return content.strip().startswith(prefix)

def format_duration(seconds: int) -> str:
    if seconds < 60:
        return f"{seconds}s"
    elif seconds < 3600:
        return f"{seconds // 60}m {seconds % 60}s"
    else:
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        return f"{hours}h {minutes}m"

# -------------------------
# Permissions helpers
# -------------------------
def is_power_user(user: discord.abc.User) -> bool:
    return user.id in POWER_USER_IDS

def admin_or_power(ctx: commands.Context) -> bool:
    if is_power_user(ctx.author):
        return True
    return bool(ctx.guild and ctx.author.guild_permissions.administrator)

# -------------------------
# Migration
# -------------------------
async def needs_migration(db: aiosqlite.Connection) -> bool:
    async with db.execute("SELECT COUNT(*) FROM terms") as cur:
        row = await cur.fetchone()
        return (row[0] or 0) == 0

async def migrate_json(db: aiosqlite.Connection):
    if not os.path.exists(JSON_PATH):
        log.info("No JSON file to migrate (%s not found).", JSON_PATH)
        return
    try:
        with open(JSON_PATH, "r") as f:
            data = json.load(f)
    except Exception as e:
        log.warning("Failed to read %s: %s", JSON_PATH, e)
        return

    term_data = data.get("term_data", {})
    tracked_terms = [normalize_term(t) for t in data.get("tracked_terms", [])]

    # Insert terms (guild_id=0 for global because JSON had no guilds)
    for term in tracked_terms or term_data.keys():
        norm = normalize_term(term)
        await db.execute("INSERT OR IGNORE INTO terms(guild_id, term) VALUES(0, ?)", (norm,))

    # Insert aggregates + per-user counts (guild_id=0)
    for term, info in term_data.items():
        norm = normalize_term(term)
        total = int(info.get("count", 0) or 0)
        last_mentioned = info.get("last_mentioned")
        last_user = info.get("last_user")
        await db.execute(
            "INSERT OR REPLACE INTO term_meta(guild_id, term, total_count, last_mentioned, last_user) VALUES(0,?,?,?,?)",
            (norm, total, last_mentioned, last_user)
        )
        for user, cnt in (info.get("user_counts") or {}).items():
            await db.execute(
                "INSERT OR REPLACE INTO hits(guild_id, term, user_id, user_name, count, last_seen) VALUES(0,?,?,0,?,?)",
                (norm, user, int(cnt or 0), last_mentioned)
            )

    # Migrate old moderation data to new unified system
    for phrase in (data.get("forbidden_phrases") or []):
        await db.execute(
            "INSERT OR IGNORE INTO moderation_actions(guild_id, phrase, action_type) VALUES(0, ?, 'delete')",
            (normalize_term(phrase),)
        )

    for phrase in (data.get("timeout_phrases") or []):
        await db.execute(
            "INSERT OR IGNORE INTO moderation_actions(guild_id, phrase, action_type, action_value) VALUES(0, ?, 'timeout', '10')",
            (normalize_term(phrase),)
        )

    for k, v in (data.get("keyword_responses") or {}).items():
        await db.execute(
            "INSERT OR REPLACE INTO moderation_actions(guild_id, phrase, action_type, action_value) VALUES(0, ?, 'response', ?)",
            (normalize_term(k), str(v))
        )

    await db.commit()
    log.info("Migration from JSON complete.")

# -------------------------
# Bot Class
# -------------------------
class TermBot(commands.Bot):
    def __init__(self):
        super().__init__(command_prefix=COMMAND_PREFIX, intents=intents, help_command=None)
        self.db: aiosqlite.Connection | None = None
        self.patterns: Dict[int, List[Tuple[str, re.Pattern]]] = {}

    def _gid(self, message: discord.Message) -> int:
        return message.guild.id if message.guild else 0

    async def setup_hook(self) -> None:
        self.db = await aiosqlite.connect(DB_PATH)
        await init_db(self.db)
        if await needs_migration(self.db):
            await migrate_json(self.db)
        await self.refresh_patterns()

    async def get_guild_settings(self, guild_id: int) -> Dict:
        async with self.db.execute(
            "SELECT ignore_commands, case_sensitive, min_word_length, cooldown_seconds, timeout_duration FROM guild_settings WHERE guild_id=?", 
            (guild_id,)
        ) as cur:
            row = await cur.fetchone()
        
        if row:
            return {
                'ignore_commands': bool(row[0]),
                'case_sensitive': bool(row[1]),
                'min_word_length': row[2],
                'cooldown_seconds': row[3],
                'timeout_duration': row[4]
            }
        else:
            return {
                'ignore_commands': True,
                'case_sensitive': False,
                'min_word_length': 1,
                'cooldown_seconds': 0,
                'timeout_duration': 10
            }

    async def is_channel_ignored(self, guild_id: int, channel_id: int) -> bool:
        async with self.db.execute(
            "SELECT 1 FROM ignored_channels WHERE guild_id=? AND channel_id=?", 
            (guild_id, channel_id)
        ) as cur:
            return await cur.fetchone() is not None

    async def check_cooldown(self, guild_id: int, user_id: int, term: str, cooldown_seconds: int) -> bool:
        if cooldown_seconds <= 0:
            return False
            
        async with self.db.execute(
            "SELECT last_increment FROM user_cooldowns WHERE guild_id=? AND user_id=? AND term=?",
            (guild_id, user_id, term)
        ) as cur:
            row = await cur.fetchone()
            
        if not row:
            return False
            
        last_time = datetime.fromisoformat(row[0])
        now = datetime.now(timezone.utc)
        return (now - last_time).total_seconds() < cooldown_seconds

    async def update_cooldown(self, guild_id: int, user_id: int, term: str):
        now = datetime.now(timezone.utc).isoformat()
        await self.db.execute(
            "INSERT OR REPLACE INTO user_cooldowns(guild_id, user_id, term, last_increment) VALUES(?,?,?,?)",
            (guild_id, user_id, term, now)
        )

    async def refresh_patterns(self):
        self.patterns.clear()
        async with self.db.execute("SELECT guild_id, term FROM terms ORDER BY guild_id, term") as cur:
            rows = await cur.fetchall()
        
        tmp = defaultdict(list)
        for gid, term in rows:
            tmp[gid].append(term)
        
        for gid, terms in tmp.items():
            settings = await self.get_guild_settings(gid)
            filtered_terms = [t for t in terms if len(t) >= settings['min_word_length']]
            self.patterns[gid] = build_patterns(filtered_terms, settings['case_sensitive'])

    async def on_ready(self):
        log.info("Logged in as %s (%s)", self.user, self.user.id)
        log.info("Connected to %s guild(s): %s", len(self.guilds), [g.name for g in self.guilds])
        
        async with self.db.execute("SELECT COUNT(*) FROM terms") as cur:
            total_terms = (await cur.fetchone())[0]
        
        async with self.db.execute("SELECT COUNT(DISTINCT guild_id) FROM terms WHERE guild_id != 0") as cur:
            active_guilds = (await cur.fetchone())[0]
            
        log.info("Tracking %s term(s) across %s guild(s)", total_terms, active_guilds)
        await self.cleanup_orphaned_guilds()

    async def cleanup_orphaned_guilds(self):
        current_guild_ids = {g.id for g in self.guilds}
        
        async with self.db.execute("SELECT DISTINCT guild_id FROM terms WHERE guild_id != 0") as cur:
            db_guild_ids = {row[0] for row in await cur.fetchall()}
        
        orphaned = db_guild_ids - current_guild_ids
        
        if orphaned:
            log.info("Cleaning up data for %s orphaned guild(s)", len(orphaned))
            for gid in orphaned:
                for table in ['terms', 'term_meta', 'hits', 'messages', 'guild_settings', 
                             'ignored_channels', 'user_cooldowns', 'moderation_actions']:
                    await self.db.execute(f"DELETE FROM {table} WHERE guild_id = ?", (gid,))
            
            await self.db.commit()
            await self.refresh_patterns()

    async def increment(self, message: discord.Message, term: str, occurrences: int = 1):
        gid = self._gid(message)
        term = normalize_term(term)
        now = datetime.now(timezone.utc).isoformat()
        user_id = int(message.author.id)
        user_name = str(message.author)

        await self.db.execute(
            "INSERT INTO term_meta(guild_id, term, total_count, last_mentioned, last_user) "
            "VALUES(?,?,?,?,?) ON CONFLICT(guild_id, term) DO UPDATE SET "
            "total_count = term_meta.total_count + excluded.total_count, "
            "last_mentioned = excluded.last_mentioned, last_user = excluded.last_user",
            (gid, term, occurrences, now, user_name)
        )
        await self.db.execute(
            "INSERT INTO hits(guild_id, term, user_id, user_name, count, last_seen) "
            "VALUES(?,?,?,?,?,?) ON CONFLICT(guild_id, term, user_id) DO UPDATE SET "
            "count = hits.count + excluded.count, last_seen = excluded.last_seen, user_name = excluded.user_name",
            (gid, term, user_id, user_name, occurrences, now)
        )
        await self.db.execute(
            "INSERT INTO messages(guild_id, channel_id, user_id, user_name, message_id, term, content, created_at) "
            "VALUES(?,?,?,?,?,?,?,?)",
            (gid, message.channel.id, user_id, user_name, message.id, term, message.content, now)
        )

    async def on_message(self, message: discord.Message):
        if message.author.bot:
            return

        content = message.content or ""
        if not content:
            return

        gid = message.guild.id if message.guild else 0
        
        if await self.is_channel_ignored(gid, message.channel.id):
            await self.process_commands(message)
            return
        
        settings = await self.get_guild_settings(gid)
        
        if settings['ignore_commands'] and is_command_message(content, COMMAND_PREFIX):
            await self.process_commands(message)
            return

        # Check for moderation actions FIRST
        await self.check_moderation_actions(message)

        # Then check for tracked terms
        pats = self.patterns.get(gid, [])
        matched_terms = []
        
        for term, pat in pats:
            matches = pat.findall(content)
            if matches:
                if not await self.check_cooldown(gid, message.author.id, term, settings['cooldown_seconds']):
                    count = len(matches)
                    await self.increment(message, term, occurrences=count)
                    await self.update_cooldown(gid, message.author.id, term)
                    matched_terms.append((term, count))

        if matched_terms:
            await self.db.commit()
            log.debug("Matched terms in message %s: %s", message.id, matched_terms)

        await self.process_commands(message)

    async def check_moderation_actions(self, message: discord.Message):
        if not message.guild:
            return
            
        gid = message.guild.id
        content = message.content.lower()
        
        async with self.db.execute(
            "SELECT phrase, action_type, action_value FROM moderation_actions WHERE guild_id=?", 
            (gid,)
        ) as cur:
            actions = await cur.fetchall()
        
        for phrase, action_type, action_value in actions:
            if phrase in content:
                if action_type == 'delete':
                    await self._handle_delete_action(message, phrase)
                elif action_type == 'timeout':
                    await self._handle_timeout_action(message, phrase, action_value)
                elif action_type == 'response':
                    await self._handle_response_action(message, phrase, action_value)
                    break  # Only respond to first matched keyword

    async def _handle_delete_action(self, message: discord.Message, phrase: str):
        try:
            await message.delete()
            log.info("Deleted message from %s containing forbidden phrase: %s", message.author, phrase)
            try:
                await message.author.send(f"Your message in **{message.guild.name}** was deleted because it contained forbidden content.")
            except discord.Forbidden:
                pass
        except discord.Forbidden:
            log.warning("No permission to delete message in guild %s", message.guild.name)

    async def _handle_timeout_action(self, message: discord.Message, phrase: str, duration_str: str):
        try:
            duration = int(duration_str or 10)
            until = discord.utils.utcnow() + timedelta(minutes=duration)
            
            await message.author.timeout(until, reason=f"Used timeout phrase: {phrase}")
            log.info("Timed out %s for %d minutes for using phrase: %s", message.author, duration, phrase)
            
            try:
                await message.delete()
            except discord.Forbidden:
                pass
            
            embed = discord.Embed(
                title="User Timed Out", 
                description=f"**{message.author.mention}** has been timed out for **{duration} minutes**",
                color=0xe74c3c
            )
            embed.add_field(name="Reason", value=f"Used timeout phrase: `{phrase}`", inline=False)
            await message.channel.send(embed=embed, delete_after=10)
            
        except (discord.Forbidden, discord.HTTPException) as e:
            log.error("Failed to timeout user %s: %s", message.author, e)

    async def _handle_response_action(self, message: discord.Message, phrase: str, response: str):
        await message.channel.send(response)
        log.debug("Sent auto-response for phrase '%s' in guild %s", phrase, message.guild.name)

bot = TermBot()

# -------------------------
# Standardized Commands
# -------------------------

# Basic tracking commands
@bot.command(name="terms")
async def cmd_terms(ctx: commands.Context):
    """List all tracked terms for this server"""
    gid = ctx.guild.id if ctx.guild else 0
    terms = []
    async with bot.db.execute("SELECT term FROM terms WHERE guild_id=? ORDER BY term", (gid,)) as cur:
        async for row in cur:
            terms.append(row[0])
    
    if not terms:
        await ctx.send("No tracked terms yet. Use `!track <term>` to add one.")
    else:
        terms_text = ", ".join(f"`{t}`" for t in terms)
        if len(terms_text) > 1900:
            chunks = []
            current_chunk = "**Tracked terms:** "
            for term in terms:
                term_formatted = f"`{term}`, "
                if len(current_chunk + term_formatted) > 1900:
                    chunks.append(current_chunk.rstrip(", "))
                    current_chunk = term_formatted
                else:
                    current_chunk += term_formatted
            if current_chunk.strip():
                chunks.append(current_chunk.rstrip(", "))
            
            for chunk in chunks:
                await ctx.send(chunk)
        else:
            await ctx.send("**Tracked terms:** " + terms_text)

@bot.command(name="track")
@commands.check(admin_or_power)
async def cmd_track(ctx: commands.Context, *, term: str):
    """Add a term to track in this server"""
    gid = ctx.guild.id if ctx.guild else 0
    term = normalize_term(term)
    
    if len(term) == 0:
        await ctx.send("Term cannot be empty.")
        return
    
    async with bot.db.execute("SELECT 1 FROM terms WHERE guild_id=? AND term=?", (gid, term)) as cur:
        exists = await cur.fetchone()
    
    if exists:
        await ctx.send(f"Already tracking `{term}`.")
        return
        
    await bot.db.execute("INSERT OR IGNORE INTO terms(guild_id, term) VALUES(?, ?)", (gid, term))
    await bot.db.commit()
    await bot.refresh_patterns()
    await ctx.send(f"Now tracking `{term}`.")

@bot.command(name="untrack")
@commands.check(admin_or_power)
async def cmd_untrack(ctx: commands.Context, *, term: str):
    """Remove a term from tracking and delete its data"""
    gid = ctx.guild.id if ctx.guild else 0
    term = normalize_term(term)
    
    async with bot.db.execute("SELECT total_count FROM term_meta WHERE guild_id=? AND term=?", (gid, term)) as cur:
        row = await cur.fetchone()
    
    if not row:
        await ctx.send(f"`{term}` is not being tracked.")
        return
    
    total_count = row[0] or 0
    
    for table in ['terms', 'term_meta', 'hits', 'messages', 'user_cooldowns']:
        await bot.db.execute(f"DELETE FROM {table} WHERE guild_id=? AND term=?", (gid, term))
    
    await bot.db.commit()
    await bot.refresh_patterns()
    await ctx.send(f"Removed `{term}` from tracked terms and deleted its stats ({total_count} total mentions).")

# Statistics commands
@bot.command(name="stats")
async def cmd_stats(ctx: commands.Context, *, term: str = None):
    """Show statistics for a term or all terms"""
    gid = ctx.guild.id if ctx.guild else 0
    if term:
        await _show_term_stats(ctx, gid, normalize_term(term))
    else:
        await _show_all_stats(ctx, gid)

async def _show_term_stats(ctx: commands.Context, gid: int, term: str):
    async with bot.db.execute(
        "SELECT total_count, last_mentioned, last_user FROM term_meta WHERE guild_id=? AND term=?", 
        (gid, term)
    ) as cur:
        meta_row = await cur.fetchone()
        
    if not meta_row:
        await ctx.send(f"No data yet for `{term}`.")
        return
        
    total_count, last_mentioned, last_user = meta_row
    
    rows = []
    async with bot.db.execute(
        "SELECT user_name, count, last_seen FROM hits WHERE guild_id=? AND term=? ORDER BY count DESC LIMIT 5", 
        (gid, term)
    ) as cur:
        async for r in cur:
            rows.append(r)
    
    embed = discord.Embed(title=f"Stats for `{term}`", color=0x3498db)
    embed.add_field(name="Total Mentions", value=total_count, inline=True)
    
    if last_mentioned and last_user:
        try:
            last_time = datetime.fromisoformat(last_mentioned.replace('Z', '+00:00'))
            embed.add_field(name="Last Mentioned", value=f"<t:{int(last_time.timestamp())}:R> by {last_user}", inline=False)
        except:
            embed.add_field(name="Last Mentioned", value=f"by {last_user}", inline=False)
    
    if rows:
        top_users = "\n".join([f"{i+1}. **{user}** — {count}" for i, (user, count, _) in enumerate(rows)])
        embed.add_field(name="Top Users", value=top_users, inline=False)
    
    await ctx.send(embed=embed)

async def _show_all_stats(ctx: commands.Context, gid: int):
    rows = []
    async with bot.db.execute(
        "SELECT term, total_count FROM term_meta WHERE guild_id=? ORDER BY total_count DESC LIMIT 10", 
        (gid,)
    ) as cur:
        async for r in cur:
            rows.append(r)
            
    if not rows:
        await ctx.send("No data yet.")
        return
        
    embed = discord.Embed(title="Top Terms", color=0x3498db)
    top_terms = "\n".join([f"{i+1}. `{t}` — {c}" for i, (t, c) in enumerate(rows)])
    embed.description = top_terms
    await ctx.send(embed=embed)

# Moderation commands - Unified system
@bot.command(name="moderation", aliases=["mod"])
async def cmd_moderation(ctx: commands.Context, action: str = None):
    """Show all moderation actions or help"""
    gid = ctx.guild.id if ctx.guild else 0
    
    if not action:
        # Show all moderation actions
        actions = []
        async with bot.db.execute(
            "SELECT phrase, action_type, action_value FROM moderation_actions WHERE guild_id=? ORDER BY action_type, phrase", 
            (gid,)
        ) as cur:
            async for phrase, action_type, action_value in cur:
                if action_type == 'delete':
                    actions.append(f"🗑️ `{phrase}` → Delete message")
                elif action_type == 'timeout':
                    duration = action_value or "10"
                    actions.append(f"⏰ `{phrase}` → Timeout for {duration} minutes")
                elif action_type == 'response':
                    actions.append(f"💬 `{phrase}` → {action_value}")
        
        if not actions:
            await ctx.send("No moderation actions configured.\n\nUse `!mod help` to see available commands.")
        else:
            embed = discord.Embed(title="Moderation Actions", color=0xe74c3c)
            embed.description = "\n".join(actions[:20])  # Limit for embed size
            if len(actions) > 20:
                embed.set_footer(text=f"Showing first 20 of {len(actions)} actions")
            await ctx.send(embed=embed)
    
    elif action == "help":
        embed = discord.Embed(title="Moderation Commands", color=0x3498db)
        embed.description = """
**Available Actions:**
• `delete` - Delete messages containing the phrase
• `timeout <minutes>` - Timeout users for specified duration
• `response <text>` - Send automatic response

**Commands:**
• `!mod set <phrase> delete` - Delete messages with phrase
• `!mod set <phrase> timeout 30` - Timeout for 30 minutes
• `!mod set <phrase> response Hello!` - Auto-respond with "Hello!"
• `!mod remove <phrase>` - Remove moderation action
• `!mod` - List all actions
        """
        await ctx.send(embed=embed)

@bot.command(name="mod_set", aliases=["modset"])
@commands.check(admin_or_power)
async def cmd_mod_set(ctx: commands.Context, phrase: str, action_type: str, *, action_value: str = None):
    """Set a moderation action for a phrase"""
    gid = ctx.guild.id if ctx.guild else 0
    phrase = normalize_term(phrase)
    action_type = action_type.lower()
    
    if action_type not in ['delete', 'timeout', 'response']:
        await ctx.send("Invalid action type. Use: delete, timeout, or response")
        return
    
    if action_type == 'timeout':
        try:
            duration = int(action_value) if action_value else 10
            if duration < 1 or duration > 40320:
                await ctx.send("Timeout duration must be between 1 and 40320 minutes (28 days).")
                return
            action_value = str(duration)
        except (ValueError, TypeError):
            await ctx.send("Invalid timeout duration. Please provide a number of minutes.")
            return
    
    elif action_type == 'response' and not action_value:
        await ctx.send("Response action requires response text.")
        return
    
    await bot.db.execute(
        "INSERT OR REPLACE INTO moderation_actions(guild_id, phrase, action_type, action_value) VALUES(?,?,?,?)",
        (gid, phrase, action_type, action_value)
    )
    await bot.db.commit()
    
    if action_type == 'delete':
        await ctx.send(f"Set to delete messages containing `{phrase}`.")
    elif action_type == 'timeout':
        await ctx.send(f"Set to timeout users for {action_value} minutes when they use `{phrase}`.")
    elif action_type == 'response':
        await ctx.send(f"Set auto-response for `{phrase}`: {action_value}")

@bot.command(name="mod_remove", aliases=["modremove"])
@commands.check(admin_or_power)
async def cmd_mod_remove(ctx: commands.Context, *, phrase: str):
    """Remove a moderation action"""
    gid = ctx.guild.id if ctx.guild else 0
    phrase = normalize_term(phrase)
    
    cur = await bot.db.execute(
        "DELETE FROM moderation_actions WHERE guild_id=? AND phrase=?",
        (gid, phrase)
    )
    await bot.db.commit()
    
    if cur.rowcount:
        await ctx.send(f"Removed moderation action for `{phrase}`.")
    else:
        await ctx.send(f"No moderation action was set for `{phrase}`.")

# Additional commands
@bot.command(name="recent")
async def cmd_recent(ctx: commands.Context, term: str = None, limit: int = 5):
    """Show recent messages containing a term"""
    gid = ctx.guild.id if ctx.guild else 0
    limit = max(1, min(20, limit))
    
    if term:
        term = normalize_term(term)
        async with bot.db.execute(
            "SELECT user_name, content, created_at, channel_id FROM messages WHERE guild_id=? AND term=? ORDER BY created_at DESC LIMIT ?",
            (gid, term, limit)
        ) as cur:
            rows = await cur.fetchall()
    else:
        async with bot.db.execute(
            "SELECT user_name, content, created_at, channel_id, term FROM messages WHERE guild_id=? ORDER BY created_at DESC LIMIT ?",
            (gid, limit)
        ) as cur:
            rows = await cur.fetchall()
    
    if not rows:
        target = f"for `{term}`" if term else ""
        await ctx.send(f"No recent messages {target}.")
        return
    
    embed = discord.Embed(
        title=f"Recent Messages" + (f" for `{term}`" if term else ""), 
        color=0x9b59b6
    )
    
    for row in rows:
        if term:
            user_name, content, created_at, channel_id = row
            term_display = term
        else:
            user_name, content, created_at, channel_id, term_display = row
            
        try:
            timestamp = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
            time_str = f"<t:{int(timestamp.timestamp())}:R>"
        except:
            time_str = "recently"
            
        display_content = content[:100] + "..." if len(content) > 100 else content
        
        channel = ctx.guild.get_channel(channel_id) if ctx.guild else None
        channel_name = f"#{channel.name}" if channel else "DM"
        
        embed.add_field(
            name=f"**{user_name}** in {channel_name}" + (f" (`{term_display}`)" if not term else ""),
            value=f"{display_content}\n{time_str}",
            inline=False
        )
    
    await ctx.send(embed=embed)

@bot.command(name="leaderboard", aliases=["lb", "top"])
async def cmd_leaderboard(ctx: commands.Context, timeframe: str = "all"):
    """Show user leaderboard for all terms"""
    gid = ctx.guild.id if ctx.guild else 0
    
    where_clause = ""
    params = [gid]
    
    if timeframe.lower() in ["day", "daily", "1d"]:
        cutoff = datetime.now(timezone.utc) - timedelta(days=1)
        where_clause = " AND last_seen >= ?"
        params.append(cutoff.isoformat())
    elif timeframe.lower() in ["week", "weekly", "1w"]:
        cutoff = datetime.now(timezone.utc) - timedelta(weeks=1)
        where_clause = " AND last_seen >= ?"
        params.append(cutoff.isoformat())
    elif timeframe.lower() in ["month", "monthly", "1m"]:
        cutoff = datetime.now(timezone.utc) - timedelta(days=30)
        where_clause = " AND last_seen >= ?"
        params.append(cutoff.isoformat())
    
    query = f"""
        SELECT user_name, SUM(count) as total 
        FROM hits 
        WHERE guild_id=?{where_clause}
        GROUP BY user_id, user_name 
        ORDER BY total DESC 
        LIMIT 10
    """
    
    rows = []
    async with bot.db.execute(query, params) as cur:
        async for r in cur:
            rows.append(r)
    
    if not rows:
        await ctx.send(f"No data for timeframe: {timeframe}")
        return
    
    embed = discord.Embed(
        title=f"Leaderboard ({timeframe.title()})",
        color=0xf1c40f
    )
    
    leaderboard = []
    for i, (user, total) in enumerate(rows):
        if i == 0:
            emoji = "🥇"
        elif i == 1:
            emoji = "🥈"
        elif i == 2:
            emoji = "🥉"
        else:
            emoji = f"{i+1}."
        leaderboard.append(f"{emoji} **{user}** — {total}")
    
    embed.description = "\n".join(leaderboard)
    await ctx.send(embed=embed)

@bot.command(name="search")
async def cmd_search(ctx: commands.Context, *, query: str):
    """Search for messages containing specific text"""
    gid = ctx.guild.id if ctx.guild else 0
    
    if len(query) < 2:
        await ctx.send("Search query must be at least 2 characters.")
        return
    
    search_query = f"%{query.lower()}%"
    
    async with bot.db.execute(
        """SELECT user_name, content, created_at, channel_id, term 
           FROM messages 
           WHERE guild_id=? AND LOWER(content) LIKE ? 
           ORDER BY created_at DESC 
           LIMIT 10""",
        (gid, search_query)
    ) as cur:
        rows = await cur.fetchall()
    
    if not rows:
        await ctx.send(f"No messages found containing: `{query}`")
        return
    
    embed = discord.Embed(
        title=f"Search Results for: `{query}`",
        color=0xe74c3c
    )
    
    for user_name, content, created_at, channel_id, term in rows:
        try:
            timestamp = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
            time_str = f"<t:{int(timestamp.timestamp())}:R>"
        except:
            time_str = "recently"
        
        display_content = content[:150] + "..." if len(content) > 150 else content
        
        channel = ctx.guild.get_channel(channel_id) if ctx.guild else None
        channel_name = f"#{channel.name}" if channel else "DM"
        
        embed.add_field(
            name=f"**{user_name}** in {channel_name} (`{term}`)",
            value=f"{display_content}\n{time_str}",
            inline=False
        )
    
    await ctx.send(embed=embed)

# Settings commands
@bot.command(name="settings")
@commands.check(admin_or_power)
async def cmd_settings(ctx: commands.Context):
    """Show current guild settings"""
    gid = ctx.guild.id if ctx.guild else 0
    settings = await bot.get_guild_settings(gid)
    
    embed = discord.Embed(title="Guild Settings", color=0x2ecc71)
    embed.add_field(name="Ignore Commands", value="Yes" if settings['ignore_commands'] else "No", inline=True)
    embed.add_field(name="Case Sensitive", value="Yes" if settings['case_sensitive'] else "No", inline=True)
    embed.add_field(name="Min Word Length", value=settings['min_word_length'], inline=True)
    embed.add_field(name="Cooldown", value=f"{settings['cooldown_seconds']}s" if settings['cooldown_seconds'] > 0 else "Disabled", inline=True)
    embed.add_field(name="Timeout Duration", value=f"{settings['timeout_duration']} minutes", inline=True)
    
    ignored = []
    async with bot.db.execute("SELECT channel_id FROM ignored_channels WHERE guild_id=?", (gid,)) as cur:
        async for row in cur:
            channel = ctx.guild.get_channel(row[0]) if ctx.guild else None
            if channel:
                ignored.append(f"#{channel.name}")
    
    if ignored:
        embed.add_field(name="Ignored Channels", value=", ".join(ignored), inline=False)
    
    await ctx.send(embed=embed)

@bot.command(name="set")
@commands.check(admin_or_power)
async def cmd_set(ctx: commands.Context, setting: str, value: str):
    """Change a guild setting"""
    gid = ctx.guild.id if ctx.guild else 0
    setting = setting.lower()
    
    await bot.db.execute("INSERT OR IGNORE INTO guild_settings(guild_id) VALUES(?)", (gid,))
    
    if setting in ["ignore_commands", "ignore_command"]:
        bool_val = value.lower() in ["true", "yes", "1", "on", "enable"]
        await bot.db.execute("UPDATE guild_settings SET ignore_commands=? WHERE guild_id=?", (bool_val, gid))
        await ctx.send(f"Commands will {'not ' if not bool_val else ''}be ignored for term tracking.")
        
    elif setting in ["case_sensitive", "case"]:
        bool_val = value.lower() in ["true", "yes", "1", "on", "enable"]
        await bot.db.execute("UPDATE guild_settings SET case_sensitive=? WHERE guild_id=?", (bool_val, gid))
        await bot.refresh_patterns()
        await ctx.send(f"Term matching is now {'case sensitive' if bool_val else 'case insensitive'}.")
        
    elif setting in ["min_word_length", "min_length", "minlength"]:
        try:
            int_val = int(value)
            if int_val < 1:
                await ctx.send("Minimum word length must be at least 1.")
                return
            await bot.db.execute("UPDATE guild_settings SET min_word_length=? WHERE guild_id=?", (int_val, gid))
            await bot.refresh_patterns()
            await ctx.send(f"Minimum word length set to {int_val}.")
        except ValueError:
            await ctx.send("Please provide a valid number.")
            
    elif setting in ["cooldown", "cooldown_seconds"]:
        try:
            int_val = int(value)
            if int_val < 0:
                await ctx.send("Cooldown cannot be negative.")
                return
            await bot.db.execute("UPDATE guild_settings SET cooldown_seconds=? WHERE guild_id=?", (int_val, gid))
            if int_val == 0:
                await ctx.send("Cooldown disabled.")
            else:
                await ctx.send(f"Cooldown set to {format_duration(int_val)}.")
        except ValueError:
            await ctx.send("Please provide a valid number of seconds.")
            
    elif setting in ["timeout_duration", "timeout", "timeout_minutes"]:
        try:
            int_val = int(value)
            if int_val < 1 or int_val > 40320:
                await ctx.send("Timeout duration must be between 1 and 40320 minutes (28 days).")
                return
            await bot.db.execute("UPDATE guild_settings SET timeout_duration=? WHERE guild_id=?", (int_val, gid))
            await ctx.send(f"Timeout duration set to {int_val} minutes.")
        except ValueError:
            await ctx.send("Please provide a valid number of minutes.")
    else:
        await ctx.send("Unknown setting. Available: ignore_commands, case_sensitive, min_word_length, cooldown, timeout_duration")
        return
    
    await bot.db.commit()

@bot.command(name="ignore_channel")
@commands.check(admin_or_power)
async def cmd_ignore_channel(ctx: commands.Context, channel: discord.TextChannel = None):
    """Add a channel to ignore list"""
    if not channel:
        channel = ctx.channel
    
    gid = ctx.guild.id if ctx.guild else 0
    
    if await bot.is_channel_ignored(gid, channel.id):
        await ctx.send(f"#{channel.name} is already being ignored.")
        return
    
    await bot.db.execute("INSERT INTO ignored_channels(guild_id, channel_id) VALUES(?, ?)", (gid, channel.id))
    await bot.db.commit()
    await ctx.send(f"Now ignoring #{channel.name} for term tracking.")

@bot.command(name="unignore_channel")
@commands.check(admin_or_power)
async def cmd_unignore_channel(ctx: commands.Context, channel: discord.TextChannel = None):
    """Remove a channel from ignore list"""
    if not channel:
        channel = ctx.channel
    
    gid = ctx.guild.id if ctx.guild else 0
    
    cur = await bot.db.execute("DELETE FROM ignored_channels WHERE guild_id=? AND channel_id=?", (gid, channel.id))
    await bot.db.commit()
    
    if cur.rowcount:
        await ctx.send(f"No longer ignoring #{channel.name}.")
    else:
        await ctx.send(f"#{channel.name} wasn't being ignored.")

@bot.command(name="reset")
@commands.check(admin_or_power)
async def cmd_reset(ctx: commands.Context, *, term: str = None):
    """Reset statistics for a term or all terms"""
    gid = ctx.guild.id if ctx.guild else 0
    
    if term:
        term = normalize_term(term)
        
        async with bot.db.execute("SELECT total_count FROM term_meta WHERE guild_id=? AND term=?", (gid, term)) as cur:
            row = await cur.fetchone()
        
        if not row:
            await ctx.send(f"`{term}` is not being tracked.")
            return
        
        total_count = row[0] or 0
        
        for table in ['term_meta', 'hits', 'messages', 'user_cooldowns']:
            await bot.db.execute(f"DELETE FROM {table} WHERE guild_id=? AND term=?", (gid, term))
        
        await bot.db.commit()
        await ctx.send(f"Reset statistics for `{term}` ({total_count} mentions cleared).")
    else:
        async with bot.db.execute("SELECT COUNT(*) FROM messages WHERE guild_id=?", (gid,)) as cur:
            total_messages = (await cur.fetchone())[0]
        
        for table in ['term_meta', 'hits', 'messages', 'user_cooldowns']:
            await bot.db.execute(f"DELETE FROM {table} WHERE guild_id=?", (gid,))
        
        await bot.db.commit()
        await ctx.send(f"Reset all statistics for this server ({total_messages} total messages cleared).")

# Import/Export commands
@bot.command(name="export")
@commands.check(admin_or_power)
async def cmd_export(ctx: commands.Context):
    """Export all data for this server"""
    import tempfile
    gid = ctx.guild.id if ctx.guild else 0

    # Collect all data
    terms = []
    async with bot.db.execute("SELECT term FROM terms WHERE guild_id=? ORDER BY term", (gid,)) as cur:
        async for r in cur:
            terms.append(r[0])

    term_data = {}
    for term in terms:
        async with bot.db.execute(
            "SELECT total_count, last_mentioned, last_user FROM term_meta WHERE guild_id=? AND term=?",
            (gid, term)
        ) as cur:
            meta = await cur.fetchone()
        
        if meta:
            total, last_mentioned, last_user = meta
        else:
            total, last_mentioned, last_user = 0, None, None
        
        users = []
        async with bot.db.execute(
            "SELECT user_id, user_name, count, last_seen FROM hits WHERE guild_id=? AND term=? ORDER BY count DESC",
            (gid, term)
        ) as cur:
            async for uid, uname, cnt, last_seen in cur:
                users.append({"user_id": uid, "user_name": uname, "count": cnt, "last_seen": last_seen})
        
        term_data[term] = {
            "count": total,
            "last_mentioned": last_mentioned,
            "last_user": last_user,
            "user_counts": users,
        }

    # Moderation actions
    moderation = []
    async with bot.db.execute(
        "SELECT phrase, action_type, action_value FROM moderation_actions WHERE guild_id=? ORDER BY phrase", 
        (gid,)
    ) as cur:
        async for phrase, action_type, action_value in cur:
            moderation.append({"phrase": phrase, "action_type": action_type, "action_value": action_value})

    # Settings and ignored channels
    settings = await bot.get_guild_settings(gid)
    
    ignored_channels = []
    async with bot.db.execute("SELECT channel_id FROM ignored_channels WHERE guild_id=?", (gid,)) as cur:
        async for r in cur:
            ignored_channels.append(r[0])

    payload = {
        "guild_id": gid,
        "export_date": datetime.now(timezone.utc).isoformat(),
        "tracked_terms": terms,
        "term_data": term_data,
        "moderation_actions": moderation,
        "settings": settings,
        "ignored_channels": ignored_channels
    }

    guild_name = ctx.guild.name if ctx.guild else "DM"
    filename = f"termbot-export-{guild_name}-{gid}.json"
    
    with tempfile.NamedTemporaryFile("w", delete=False, suffix=".json") as fp:
        json.dump(payload, fp, indent=2)
        tmp_path = fp.name
    
    await ctx.send("Server data export:", file=discord.File(tmp_path, filename=filename))
    os.unlink(tmp_path)

@bot.command(name="import")
@commands.check(admin_or_power)
async def cmd_import(ctx: commands.Context):
    """Import data from an export file"""
    gid = ctx.guild.id if ctx.guild else 0
    if not ctx.message.attachments:
        await ctx.send("Attach a JSON export file to import.")
        return
    
    try:
        data_bytes = await ctx.message.attachments[0].read()
        data = json.loads(data_bytes.decode("utf-8"))
    except Exception as e:
        await ctx.send(f"Couldn't parse JSON: {e}")
        return

    # Import terms
    for term in (data.get("tracked_terms") or []):
        term = normalize_term(term)
        await bot.db.execute("INSERT OR IGNORE INTO terms(guild_id, term) VALUES(?, ?)", (gid, term))

    # Import term data
    term_data = data.get("term_data") or {}
    for term, info in term_data.items():
        term = normalize_term(term)
        total = int(info.get("count", 0) or 0)
        last_mentioned = info.get("last_mentioned")
        last_user = info.get("last_user")
        await bot.db.execute(
            "INSERT INTO term_meta(guild_id, term, total_count, last_mentioned, last_user) VALUES(?,?,?,?,?) "
            "ON CONFLICT(guild_id, term) DO UPDATE SET total_count=excluded.total_count, last_mentioned=excluded.last_mentioned, last_user=excluded.last_user",
            (gid, term, total, last_mentioned, last_user)
        )
        
        # Handle user counts (both old dict format and new list format)
        uc = info.get("user_counts") or {}
        if isinstance(uc, dict):
            for uname, cnt in uc.items():
                await bot.db.execute(
                    "INSERT INTO hits(guild_id, term, user_id, user_name, count, last_seen) VALUES(?,?,?,?,?,NULL) "
                    "ON CONFLICT(guild_id, term, user_id) DO UPDATE SET count=excluded.count, user_name=excluded.user_name",
                    (gid, term, 0, str(uname), int(cnt or 0))
                )
        else:
            for item in uc:
                uid = int(item.get("user_id", 0) or 0)
                uname = str(item.get("user_name", uid))
                cnt = int(item.get("count", 0) or 0)
                last_seen = item.get("last_seen")
                await bot.db.execute(
                    "INSERT INTO hits(guild_id, term, user_id, user_name, count, last_seen) VALUES(?,?,?,?,?,?) "
                    "ON CONFLICT(guild_id, term, user_id) DO UPDATE SET count=excluded.count, user_name=excluded.user_name, last_seen=excluded.last_seen",
                    (gid, term, uid, uname, cnt, last_seen)
                )

    # Import moderation actions
    for action in (data.get("moderation_actions") or []):
        phrase = normalize_term(action.get("phrase", ""))
        action_type = action.get("action_type")
        action_value = action.get("action_value")
        if phrase and action_type:
            await bot.db.execute(
                "INSERT OR REPLACE INTO moderation_actions(guild_id, phrase, action_type, action_value) VALUES(?,?,?,?)",
                (gid, phrase, action_type, action_value)
            )

    # Import settings
    settings_data = data.get("settings", {})
    if settings_data:
        await bot.db.execute(
            "INSERT OR REPLACE INTO guild_settings(guild_id, ignore_commands, case_sensitive, min_word_length, cooldown_seconds, timeout_duration) VALUES(?,?,?,?,?,?)",
            (gid, settings_data.get("ignore_commands", True), settings_data.get("case_sensitive", False), 
             settings_data.get("min_word_length", 1), settings_data.get("cooldown_seconds", 0), 
             settings_data.get("timeout_duration", 10))
        )

    # Import ignored channels (only if they still exist)
    if ctx.guild:
        for channel_id in (data.get("ignored_channels") or []):
            if ctx.guild.get_channel(channel_id):
                await bot.db.execute("INSERT OR IGNORE INTO ignored_channels(guild_id, channel_id) VALUES(?, ?)", (gid, channel_id))

    await bot.db.commit()
    await bot.refresh_patterns()
    await ctx.send("Import complete for this server.")

# Help command
@bot.command(name="help", aliases=["h"])
async def cmd_help(ctx: commands.Context, command: str = None):
    """Show help information"""
    if command:
        cmd_obj = bot.get_command(command)
        if not cmd_obj:
            await ctx.send(f"Unknown command: `{command}`")
            return
        
        embed = discord.Embed(title=f"Help: `{COMMAND_PREFIX}{command}`", color=0x3498db)
        embed.description = cmd_obj.help or "No description available"
        
        if hasattr(cmd_obj, 'aliases') and cmd_obj.aliases:
            embed.add_field(name="Aliases", value=", ".join(f"`{a}`" for a in cmd_obj.aliases), inline=False)
        
        await ctx.send(embed=embed)
        return
    
    embed = discord.Embed(title="Term Tracker Bot Help", color=0x3498db)
    embed.description = "Track and count mentions of specific terms across your server!"
    
    basic = [
        f"`{COMMAND_PREFIX}terms` - List tracked terms",
        f"`{COMMAND_PREFIX}stats [term]` - Show statistics",
        f"`{COMMAND_PREFIX}recent [term] [limit]` - Show recent messages",
        f"`{COMMAND_PREFIX}leaderboard [timeframe]` - User rankings",
        f"`{COMMAND_PREFIX}search <query>` - Search messages"
    ]
    embed.add_field(name="Basic Commands", value="\n".join(basic), inline=False)
    
    admin = [
        f"`{COMMAND_PREFIX}track <term>` - Start tracking a term",
        f"`{COMMAND_PREFIX}untrack <term>` - Stop tracking a term",
        f"`{COMMAND_PREFIX}settings` - Show guild settings",
        f"`{COMMAND_PREFIX}set <setting> <value>` - Change settings",
        f"`{COMMAND_PREFIX}ignore_channel [channel]` - Ignore channel",
        f"`{COMMAND_PREFIX}reset [term]` - Reset statistics"
    ]
    embed.add_field(name="Admin Commands", value="\n".join(admin), inline=False)
    
    moderation = [
        f"`{COMMAND_PREFIX}mod` - List all moderation actions",
        f"`{COMMAND_PREFIX}mod help` - Show moderation help",
        f"`{COMMAND_PREFIX}mod_set <phrase> <action> [value]` - Set action",
        f"`{COMMAND_PREFIX}mod_remove <phrase>` - Remove action"
    ]
    embed.add_field(name="Moderation Commands", value="\n".join(moderation), inline=False)
    
    embed.add_field(name="Example Moderation Setup", 
                    value=f"`{COMMAND_PREFIX}mod_set \"who is\" response \"I don't know, who are you asking about?\"`", 
                    inline=False)
    
    embed.set_footer(text=f"Use {COMMAND_PREFIX}help <command> for detailed help")
    await ctx.send(embed=embed)

# Power user commands
@bot.command(name="pu_guilds")
@commands.check(lambda ctx: is_power_user(ctx.author))
async def cmd_pu_guilds(ctx: commands.Context):
    """[Power User] List all guilds the bot is in"""
    lines = [f"{g.name} — {g.id} ({g.member_count} members)" for g in bot.guilds]
    if not lines:
        await ctx.send("Bot is not in any guilds.")
    else:
        embed = discord.Embed(title="Bot Guilds", color=0xe67e22)
        embed.description = "\n".join(lines[:50])
        if len(lines) > 50:
            embed.set_footer(text=f"Showing first 50 of {len(lines)} guilds")
        await ctx.send(embed=embed)

@bot.command(name="pu_all_terms")
@commands.check(lambda ctx: is_power_user(ctx.author))
async def cmd_pu_all_terms(ctx: commands.Context, limit: int = 20):
    """[Power User] Show global term statistics"""
    limit = max(1, min(100, limit))
    rows = []
    async with bot.db.execute(
        "SELECT term, SUM(total_count) AS total FROM term_meta GROUP BY term ORDER BY total DESC LIMIT ?",
        (limit,)
    ) as cur:
        async for r in cur:
            rows.append(r)
    
    if not rows:
        await ctx.send("No data yet.")
        return
    
    embed = discord.Embed(title="Global Top Terms", color=0x9b59b6)
    lines = [f"{i+1}. `{t}` — {c:,}" for i, (t, c) in enumerate(rows)]
    embed.description = "\n".join(lines)
    await ctx.send(embed=embed)

# Error handling
@bot.event
async def on_command_error(ctx: commands.Context, error):
    if isinstance(error, commands.CommandNotFound):
        return
    elif isinstance(error, commands.CheckFailure):
        await ctx.send("You don't have permission to use this command.")
    elif isinstance(error, commands.MissingRequiredArgument):
        await ctx.send(f"Missing required argument. Use `{COMMAND_PREFIX}help {ctx.command.name}` for usage.")
    elif isinstance(error, commands.BadArgument):
        await ctx.send(f"Invalid argument. Use `{COMMAND_PREFIX}help {ctx.command.name}` for usage.")
    else:
        log.error("Command error in %s: %s", ctx.command, error, exc_info=error)
        await ctx.send("An error occurred while processing your command.")

# -------------------------
# Entrypoint
# -------------------------
def main():
    token = os.getenv("DISCORD_TOKEN")
    if not token:
        raise SystemExit("DISCORD_TOKEN not set in .env")
    
    try:
        import certifi
        os.environ.setdefault("SSL_CERT_FILE", certifi.where())
        os.environ.setdefault("REQUESTS_CA_BUNDLE", certifi.where())
    except Exception:
        pass
    
    bot.run(token, reconnect=True)

if __name__ == "__main__":
    main()