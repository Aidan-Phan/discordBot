import os
import re
import json
import logging
from datetime import datetime, timezone, timedelta
from typing import List, Tuple, Dict, Optional

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

# Power users (comma-separated Discord user IDs). They bypass admin checks and can run global queries.
POWER_USER_IDS = {
    int(x) for x in os.getenv("POWER_USER_IDS", "").replace(" ", "").split(",") if x.strip().isdigit()
}

# Case-insensitive matching enforced:
CASE_SENSITIVE = False

# Path for migration (your existing JSON file)
JSON_PATH = os.getenv("JSON_PATH", "bot_data.json")

# Ignore terms in bot commands by default
IGNORE_COMMANDS = os.getenv("IGNORE_COMMANDS", "true").lower() == "true"

intents = discord.Intents.default()
intents.message_content = True
intents.messages = True
intents.guilds = True
intents.members = False  # not needed

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
-- per-guild moderation/response config
CREATE TABLE IF NOT EXISTS forbidden_phrases (
    guild_id INTEGER NOT NULL,
    phrase TEXT NOT NULL,
    PRIMARY KEY (guild_id, phrase)
);
CREATE TABLE IF NOT EXISTS timeout_phrases (
    guild_id INTEGER NOT NULL,
    phrase TEXT NOT NULL,
    PRIMARY KEY (guild_id, phrase)
);
CREATE TABLE IF NOT EXISTS keyword_responses (
    guild_id INTEGER NOT NULL,
    keyword TEXT NOT NULL,
    response TEXT NOT NULL,
    PRIMARY KEY (guild_id, keyword)
);
-- New table for ignored channels
CREATE TABLE IF NOT EXISTS ignored_channels (
    guild_id INTEGER NOT NULL,
    channel_id INTEGER NOT NULL,
    PRIMARY KEY (guild_id, channel_id)
);
-- New table for guild settings
CREATE TABLE IF NOT EXISTS guild_settings (
    guild_id INTEGER NOT NULL PRIMARY KEY,
    ignore_commands BOOLEAN DEFAULT true,
    case_sensitive BOOLEAN DEFAULT false,
    min_word_length INTEGER DEFAULT 1,
    cooldown_seconds INTEGER DEFAULT 0
);
-- New table for user cooldowns
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
        # Use word boundaries for better matching
        pattern = r'\b' + re.escape(term) + r'\b'
        patterns.append((term, re.compile(pattern, flags)))
    return patterns

def is_command_message(content: str, prefix: str) -> bool:
    """Check if message starts with command prefix"""
    return content.strip().startswith(prefix)

def format_duration(seconds: int) -> str:
    """Format seconds into human readable duration"""
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
    # Allow if power user, otherwise require guild admin
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

    # Persist forbidden phrases (guild_id=0)
    for phrase in (data.get("forbidden_phrases") or []):
        await db.execute(
            "INSERT OR IGNORE INTO forbidden_phrases(guild_id, phrase) VALUES(0, ?)",
            (normalize_term(phrase),)
        )

    # Persist timeout phrases (guild_id=0)
    for phrase in (data.get("timeout_phrases") or []):
        await db.execute(
            "INSERT OR IGNORE INTO timeout_phrases(guild_id, phrase) VALUES(0, ?)",
            (normalize_term(phrase),)
        )

    # Persist keyword responses (guild_id=0)
    for k, v in (data.get("keyword_responses") or {}).items():
        await db.execute(
            "INSERT OR REPLACE INTO keyword_responses(guild_id, keyword, response) VALUES(0, ?, ?)",
            (normalize_term(k), str(v))
        )

    await db.commit()
    log.info("Migration from JSON complete. You can keep bot_data.json as a backup or delete it later.")

# -------------------------
# Bot
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
        """Get guild-specific settings"""
        async with self.db.execute(
            "SELECT ignore_commands, case_sensitive, min_word_length, cooldown_seconds FROM guild_settings WHERE guild_id=?", 
            (guild_id,)
        ) as cur:
            row = await cur.fetchone()
        
        if row:
            return {
                'ignore_commands': bool(row[0]),
                'case_sensitive': bool(row[1]),
                'min_word_length': row[2],
                'cooldown_seconds': row[3]
            }
        else:
            # Return defaults
            return {
                'ignore_commands': True,
                'case_sensitive': False,
                'min_word_length': 1,
                'cooldown_seconds': 0
            }

    async def is_channel_ignored(self, guild_id: int, channel_id: int) -> bool:
        """Check if channel should be ignored"""
        async with self.db.execute(
            "SELECT 1 FROM ignored_channels WHERE guild_id=? AND channel_id=?", 
            (guild_id, channel_id)
        ) as cur:
            return await cur.fetchone() is not None

    async def check_cooldown(self, guild_id: int, user_id: int, term: str, cooldown_seconds: int) -> bool:
        """Check if user is on cooldown for this term"""
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
        """Update user's cooldown for this term"""
        now = datetime.now(timezone.utc).isoformat()
        await self.db.execute(
            "INSERT OR REPLACE INTO user_cooldowns(guild_id, user_id, term, last_increment) VALUES(?,?,?,?)",
            (guild_id, user_id, term, now)
        )

    async def refresh_patterns(self):
        self.patterns.clear()
        # Load all guild/term pairs
        async with self.db.execute("SELECT guild_id, term FROM terms ORDER BY guild_id, term") as cur:
            rows = await cur.fetchall()
        from collections import defaultdict
        tmp = defaultdict(list)
        for gid, term in rows:
            tmp[gid].append(term)
        
        # Build patterns for each guild with their settings
        for gid, terms in tmp.items():
            settings = await self.get_guild_settings(gid)
            # Filter terms by minimum length
            filtered_terms = [t for t in terms if len(t) >= settings['min_word_length']]
            self.patterns[gid] = build_patterns(filtered_terms, settings['case_sensitive'])

    async def on_ready(self):
        log.info("Logged in as %s (%s)", self.user, self.user.id)
        log.info("Connected to %s guild(s): %s", len(self.guilds), [g.name for g in self.guilds])
        
        # Get database stats
        async with self.db.execute("SELECT COUNT(*) FROM terms") as cur:
            total_terms = (await cur.fetchone())[0]
        
        async with self.db.execute("SELECT COUNT(DISTINCT guild_id) FROM terms WHERE guild_id != 0") as cur:
            active_guilds = (await cur.fetchone())[0]
            
        # Check for legacy data (guild_id = 0)
        async with self.db.execute("SELECT COUNT(*) FROM terms WHERE guild_id = 0") as cur:
            legacy_terms = (await cur.fetchone())[0]
        
        if legacy_terms > 0:
            log.info("Found %s legacy term(s) from migration (guild_id=0)", legacy_terms)
        
        log.info("Tracking %s term(s) across %s guild(s) with active tracking", total_terms, active_guilds)
        
        # Clean up data for guilds the bot is no longer in
        await self.cleanup_orphaned_guilds()
        
        # Show per-guild breakdown if in debug mode
        if log.isEnabledFor(logging.DEBUG):
            async with self.db.execute("""
                SELECT guild_id, COUNT(*) as term_count 
                FROM terms 
                WHERE guild_id != 0 
                GROUP BY guild_id 
                ORDER BY term_count DESC
            """) as cur:
                guild_breakdown = await cur.fetchall()
                
            for gid, count in guild_breakdown:
                guild = self.get_guild(gid)
                guild_name = guild.name if guild else f"Unknown Guild ({gid})"
                log.debug("Guild '%s': %s terms", guild_name, count)

    async def cleanup_orphaned_guilds(self):
        """Remove data for guilds the bot is no longer in"""
        current_guild_ids = {g.id for g in self.guilds}
        
        # Find guilds in database that bot is no longer in
        async with self.db.execute("SELECT DISTINCT guild_id FROM terms WHERE guild_id != 0") as cur:
            db_guild_ids = {row[0] for row in await cur.fetchall()}
        
        orphaned = db_guild_ids - current_guild_ids
        
        if orphaned:
            log.info("Cleaning up data for %s orphaned guild(s): %s", len(orphaned), list(orphaned))
            for gid in orphaned:
                await self.db.execute("DELETE FROM terms WHERE guild_id = ?", (gid,))
                await self.db.execute("DELETE FROM term_meta WHERE guild_id = ?", (gid,))
                await self.db.execute("DELETE FROM hits WHERE guild_id = ?", (gid,))
                await self.db.execute("DELETE FROM messages WHERE guild_id = ?", (gid,))
                await self.db.execute("DELETE FROM guild_settings WHERE guild_id = ?", (gid,))
                await self.db.execute("DELETE FROM ignored_channels WHERE guild_id = ?", (gid,))
                await self.db.execute("DELETE FROM user_cooldowns WHERE guild_id = ?", (gid,))
                await self.db.execute("DELETE FROM forbidden_phrases WHERE guild_id = ?", (gid,))
                await self.db.execute("DELETE FROM timeout_phrases WHERE guild_id = ?", (gid,))
                await self.db.execute("DELETE FROM keyword_responses WHERE guild_id = ?", (gid,))
            
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
        
        # Check if channel is ignored
        if await self.is_channel_ignored(gid, message.channel.id):
            await self.process_commands(message)
            return
        
        # Get guild settings
        settings = await self.get_guild_settings(gid)
        
        # Skip if it's a command and ignore_commands is enabled
        if settings['ignore_commands'] and is_command_message(content, COMMAND_PREFIX):
            await self.process_commands(message)
            return

        pats = self.patterns.get(gid, [])
        matched_terms = []
        
        for term, pat in pats:
            matches = pat.findall(content)  # Find all occurrences
            if matches:
                # Check cooldown
                if not await self.check_cooldown(gid, message.author.id, term, settings['cooldown_seconds']):
                    count = len(matches)
                    await self.increment(message, term, occurrences=count)
                    await self.update_cooldown(gid, message.author.id, term)
                    matched_terms.append((term, count))

        if matched_terms:
            await self.db.commit()
            log.debug("Matched terms in message %s: %s", message.id, matched_terms)

        await self.process_commands(message)

bot = TermBot()

# -------------------------
# Commands
# -------------------------
@bot.command(name="terms")
async def cmd_terms(ctx: commands.Context):
    """List all tracked terms for this server"""
    gid = ctx.guild.id if ctx.guild else 0
    terms: List[str] = []
    async with bot.db.execute("SELECT term FROM terms WHERE guild_id=? ORDER BY term", (gid,)) as cur:
        async for row in cur:
            terms.append(row[0])
    if not terms:
        await ctx.send("No tracked terms yet. Use `!track <term>` to add one.")
    else:
        # Split into chunks if too long
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
            
            for i, chunk in enumerate(chunks):
                if i == 0:
                    await ctx.send(chunk)
                else:
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
        await ctx.send("❌ Term cannot be empty.")
        return
    
    # Check if already tracking
    async with bot.db.execute("SELECT 1 FROM terms WHERE guild_id=? AND term=?", (gid, term)) as cur:
        exists = await cur.fetchone()
    
    if exists:
        await ctx.send(f"⚠️ Already tracking `{term}`.")
        return
        
    await bot.db.execute("INSERT OR IGNORE INTO terms(guild_id, term) VALUES(?, ?)", (gid, term))
    await bot.db.commit()
    await bot.refresh_patterns()
    await ctx.send(f"✅ Now tracking `{term}`.")

@bot.command(name="untrack")
@commands.check(admin_or_power)
async def cmd_untrack(ctx: commands.Context, *, term: str):
    """Remove a term from tracking and delete its data"""
    gid = ctx.guild.id if ctx.guild else 0
    term = normalize_term(term)
    
    # Check if exists first
    async with bot.db.execute("SELECT total_count FROM term_meta WHERE guild_id=? AND term=?", (gid, term)) as cur:
        row = await cur.fetchone()
    
    if not row:
        await ctx.send(f"❌ `{term}` is not being tracked.")
        return
    
    total_count = row[0] or 0
    
    await bot.db.execute("DELETE FROM terms WHERE guild_id=? AND term=?", (gid, term))
    await bot.db.execute("DELETE FROM term_meta WHERE guild_id=? AND term=?", (gid, term))
    await bot.db.execute("DELETE FROM hits WHERE guild_id=? AND term=?", (gid, term))
    await bot.db.execute("DELETE FROM messages WHERE guild_id=? AND term=?", (gid, term))
    await bot.db.execute("DELETE FROM user_cooldowns WHERE guild_id=? AND term=?", (gid, term))
    await bot.db.commit()
    await bot.refresh_patterns()
    await ctx.send(f"🗑️ Removed `{term}` from tracked terms and deleted its stats ({total_count} total mentions).")

@bot.command(name="stats")
async def cmd_stats(ctx: commands.Context, *, term: str = None):
    """Show statistics for a term or all terms"""
    gid = ctx.guild.id if ctx.guild else 0
    if term:
        term = normalize_term(term)
        
        # Get total count and meta info
        async with bot.db.execute(
            "SELECT total_count, last_mentioned, last_user FROM term_meta WHERE guild_id=? AND term=?", 
            (gid, term)
        ) as cur:
            meta_row = await cur.fetchone()
            
        if not meta_row:
            await ctx.send(f"No data yet for `{term}`.")
            return
            
        total_count, last_mentioned, last_user = meta_row
        
        # Get top users
        rows = []
        async with bot.db.execute(
            "SELECT user_name, count, last_seen FROM hits WHERE guild_id=? AND term=? ORDER BY count DESC LIMIT 10", 
            (gid, term)
        ) as cur:
            async for r in cur:
                rows.append(r)
        
        if not rows:
            await ctx.send(f"No user data yet for `{term}`.")
            return
        
        embed = discord.Embed(title=f"📊 Stats for `{term}`", color=0x3498db)
        embed.add_field(name="Total Mentions", value=total_count, inline=True)
        
        if last_mentioned and last_user:
            try:
                last_time = datetime.fromisoformat(last_mentioned.replace('Z', '+00:00'))
                embed.add_field(name="Last Mentioned", value=f"<t:{int(last_time.timestamp())}:R> by {last_user}", inline=False)
            except:
                embed.add_field(name="Last Mentioned", value=f"by {last_user}", inline=False)
        
        top_users = "\n".join([f"{i+1}. **{user}** — {count}" for i, (user, count, _) in enumerate(rows[:5])])
        embed.add_field(name="Top Users", value=top_users, inline=False)
        
        await ctx.send(embed=embed)
    else:
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
            
        embed = discord.Embed(title="📊 Top Terms", color=0x3498db)
        top_terms = "\n".join([f"{i+1}. `{t}` — {c}" for i, (t, c) in enumerate(rows)])
        embed.description = top_terms
        await ctx.send(embed=embed)

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
        title=f"💬 Recent Messages" + (f" for `{term}`" if term else ""), 
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
            
        # Truncate content if too long
        display_content = content[:100] + "..." if len(content) > 100 else content
        
        channel = ctx.guild.get_channel(channel_id) if ctx.guild else None
        channel_name = f"#{channel.name}" if channel else "DM"
        
        embed.add_field(
            name=f"**{user_name}** in {channel_name}" + (f" (`{term_display}`)" if not term else ""),
            value=f"{display_content}\n{time_str}",
            inline=False
        )
    
    await ctx.send(embed=embed)

# -------------------------
# Settings Commands
# -------------------------
@bot.command(name="settings")
@commands.check(admin_or_power)
async def cmd_settings(ctx: commands.Context):
    """Show current guild settings"""
    gid = ctx.guild.id if ctx.guild else 0
    settings = await bot.get_guild_settings(gid)
    
    embed = discord.Embed(title="⚙️ Guild Settings", color=0x2ecc71)
    embed.add_field(name="Ignore Commands", value="✅ Yes" if settings['ignore_commands'] else "❌ No", inline=True)
    embed.add_field(name="Case Sensitive", value="✅ Yes" if settings['case_sensitive'] else "❌ No", inline=True)
    embed.add_field(name="Min Word Length", value=settings['min_word_length'], inline=True)
    embed.add_field(name="Cooldown", value=f"{settings['cooldown_seconds']}s" if settings['cooldown_seconds'] > 0 else "Disabled", inline=True)
    
    # Show ignored channels
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
    
    # Initialize settings if they don't exist
    await bot.db.execute(
        "INSERT OR IGNORE INTO guild_settings(guild_id) VALUES(?)", (gid,)
    )
    
    if setting in ["ignore_commands", "ignore_command"]:
        bool_val = value.lower() in ["true", "yes", "1", "on", "enable"]
        await bot.db.execute(
            "UPDATE guild_settings SET ignore_commands=? WHERE guild_id=?",
            (bool_val, gid)
        )
        await ctx.send(f"✅ Commands will {'not ' if not bool_val else ''}be ignored for term tracking.")
        
    elif setting in ["case_sensitive", "case"]:
        bool_val = value.lower() in ["true", "yes", "1", "on", "enable"]
        await bot.db.execute(
            "UPDATE guild_settings SET case_sensitive=? WHERE guild_id=?",
            (bool_val, gid)
        )
        await bot.refresh_patterns()
        await ctx.send(f"✅ Term matching is now {'case sensitive' if bool_val else 'case insensitive'}.")
        
    elif setting in ["min_word_length", "min_length", "minlength"]:
        try:
            int_val = int(value)
            if int_val < 1:
                await ctx.send("❌ Minimum word length must be at least 1.")
                return
            await bot.db.execute(
                "UPDATE guild_settings SET min_word_length=? WHERE guild_id=?",
                (int_val, gid)
            )
            await bot.refresh_patterns()
            await ctx.send(f"✅ Minimum word length set to {int_val}.")
        except ValueError:
            await ctx.send("❌ Please provide a valid number.")
            
    elif setting in ["cooldown", "cooldown_seconds"]:
        try:
            int_val = int(value)
            if int_val < 0:
                await ctx.send("❌ Cooldown cannot be negative.")
                return
            await bot.db.execute(
                "UPDATE guild_settings SET cooldown_seconds=? WHERE guild_id=?",
                (int_val, gid)
            )
            if int_val == 0:
                await ctx.send("✅ Cooldown disabled.")
            else:
                await ctx.send(f"✅ Cooldown set to {format_duration(int_val)}.")
        except ValueError:
            await ctx.send("❌ Please provide a valid number of seconds.")
    else:
        await ctx.send("❌ Unknown setting. Available: ignore_commands, case_sensitive, min_word_length, cooldown")
        return
    
    await bot.db.commit()

@bot.command(name="ignore_channel")
@commands.check(admin_or_power)
async def cmd_ignore_channel(ctx: commands.Context, channel: discord.TextChannel = None):
    """Add a channel to ignore list"""
    if not channel:
        channel = ctx.channel
    
    gid = ctx.guild.id if ctx.guild else 0
    
    # Check if already ignored
    if await bot.is_channel_ignored(gid, channel.id):
        await ctx.send(f"⚠️ #{channel.name} is already being ignored.")
        return
    
    await bot.db.execute(
        "INSERT INTO ignored_channels(guild_id, channel_id) VALUES(?, ?)",
        (gid, channel.id)
    )
    await bot.db.commit()
    await ctx.send(f"✅ Now ignoring #{channel.name} for term tracking.")

@bot.command(name="unignore_channel")
@commands.check(admin_or_power)
async def cmd_unignore_channel(ctx: commands.Context, channel: discord.TextChannel = None):
    """Remove a channel from ignore list"""
    if not channel:
        channel = ctx.channel
    
    gid = ctx.guild.id if ctx.guild else 0
    
    cur = await bot.db.execute(
        "DELETE FROM ignored_channels WHERE guild_id=? AND channel_id=?",
        (gid, channel.id)
    )
    await bot.db.commit()
    
    if cur.rowcount:
        await ctx.send(f"✅ No longer ignoring #{channel.name}.")
    else:
        await ctx.send(f"⚠️ #{channel.name} wasn't being ignored.")

@bot.command(name="leaderboard", aliases=["lb", "top"])
async def cmd_leaderboard(ctx: commands.Context, timeframe: str = "all"):
    """Show user leaderboard for all terms"""
    gid = ctx.guild.id if ctx.guild else 0
    
    # Parse timeframe
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
        title=f"🏆 Leaderboard ({timeframe.title()})",
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
        await ctx.send("❌ Search query must be at least 2 characters.")
        return
    
    # Use LIKE for partial matching
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
        title=f"🔍 Search Results for: `{query}`",
        color=0xe74c3c
    )
    
    for user_name, content, created_at, channel_id, term in rows:
        try:
            timestamp = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
            time_str = f"<t:{int(timestamp.timestamp())}:R>"
        except:
            time_str = "recently"
        
        # Highlight the search term in content
        display_content = content[:150] + "..." if len(content) > 150 else content
        
        channel = ctx.guild.get_channel(channel_id) if ctx.guild else None
        channel_name = f"#{channel.name}" if channel else "DM"
        
        embed.add_field(
            name=f"**{user_name}** in {channel_name} (`{term}`)",
            value=f"{display_content}\n{time_str}",
            inline=False
        )
    
    await ctx.send(embed=embed)

@bot.command(name="reset")
@commands.check(admin_or_power)
async def cmd_reset(ctx: commands.Context, *, term: str = None):
    """Reset statistics for a term or all terms"""
    gid = ctx.guild.id if ctx.guild else 0
    
    if term:
        term = normalize_term(term)
        
        # Check if term exists
        async with bot.db.execute("SELECT total_count FROM term_meta WHERE guild_id=? AND term=?", (gid, term)) as cur:
            row = await cur.fetchone()
        
        if not row:
            await ctx.send(f"❌ `{term}` is not being tracked.")
            return
        
        total_count = row[0] or 0
        
        # Reset the stats but keep the term tracked
        await bot.db.execute("DELETE FROM term_meta WHERE guild_id=? AND term=?", (gid, term))
        await bot.db.execute("DELETE FROM hits WHERE guild_id=? AND term=?", (gid, term))
        await bot.db.execute("DELETE FROM messages WHERE guild_id=? AND term=?", (gid, term))
        await bot.db.execute("DELETE FROM user_cooldowns WHERE guild_id=? AND term=?", (gid, term))
        await bot.db.commit()
        
        await ctx.send(f"✅ Reset statistics for `{term}` ({total_count} mentions cleared).")
    else:
        # Reset all terms for this guild
        async with bot.db.execute("SELECT COUNT(*) FROM messages WHERE guild_id=?", (gid,)) as cur:
            total_messages = (await cur.fetchone())[0]
        
        await bot.db.execute("DELETE FROM term_meta WHERE guild_id=?", (gid,))
        await bot.db.execute("DELETE FROM hits WHERE guild_id=?", (gid,))
        await bot.db.execute("DELETE FROM messages WHERE guild_id=?", (gid,))
        await bot.db.execute("DELETE FROM user_cooldowns WHERE guild_id=?", (gid,))
        await bot.db.commit()
        
        await ctx.send(f"✅ Reset all statistics for this server ({total_messages} total messages cleared).")

# -------------------------
# Additional Moderation/Response Commands
# -------------------------

@bot.command(name="forbidden")
async def cmd_forbidden(ctx: commands.Context):
    """List forbidden phrases"""
    gid = ctx.guild.id if ctx.guild else 0
    phrases = []
    async with bot.db.execute("SELECT phrase FROM forbidden_phrases WHERE guild_id=? ORDER BY phrase", (gid,)) as cur:
        async for r in cur:
            phrases.append(r[0])
    if not phrases:
        await ctx.send("No forbidden phrases configured.")
    else:
        await ctx.send("**Forbidden phrases:** " + ", ".join(f"`{p}`" for p in phrases))

@bot.command(name="timeouts")
async def cmd_timeouts(ctx: commands.Context):
    """List timeout phrases"""
    gid = ctx.guild.id if ctx.guild else 0
    phrases = []
    async with bot.db.execute("SELECT phrase FROM timeout_phrases WHERE guild_id=? ORDER BY phrase", (gid,)) as cur:
        async for r in cur:
            phrases.append(r[0])
    if not phrases:
        await ctx.send("No timeout phrases configured.")
    else:
        await ctx.send("**Timeout phrases:** " + ", ".join(f"`{p}`" for p in phrases))

@bot.command(name="responses")
async def cmd_responses(ctx: commands.Context):
    """List keyword responses"""
    gid = ctx.guild.id if ctx.guild else 0
    pairs = []
    async with bot.db.execute("SELECT keyword, response FROM keyword_responses WHERE guild_id=? ORDER BY keyword", (gid,)) as cur:
        async for r in cur:
            pairs.append((r[0], r[1]))
    if not pairs:
        await ctx.send("No keyword responses configured.")
    else:
        lines = [f"`{k}` → {v}" for k, v in pairs[:25]]
        await ctx.send("**Keyword responses:**\n" + "\n".join(lines))

@bot.command(name="forbidden_add")
@commands.check(admin_or_power)
async def cmd_forbidden_add(ctx: commands.Context, *, phrase: str):
    """Add a forbidden phrase"""
    gid = ctx.guild.id if ctx.guild else 0
    phrase = normalize_term(phrase)
    await bot.db.execute("INSERT OR IGNORE INTO forbidden_phrases(guild_id, phrase) VALUES(?, ?)", (gid, phrase))
    await bot.db.commit()
    await ctx.send(f"✅ Added forbidden phrase `{phrase}` for this server.")

@bot.command(name="forbidden_remove")
@commands.check(admin_or_power)
async def cmd_forbidden_remove(ctx: commands.Context, *, phrase: str):
    """Remove a forbidden phrase"""
    gid = ctx.guild.id if ctx.guild else 0
    phrase = normalize_term(phrase)
    cur = await bot.db.execute("DELETE FROM forbidden_phrases WHERE guild_id=? AND phrase=?", (gid, phrase))
    await bot.db.commit()
    if cur.rowcount:
        await ctx.send(f"🗑️ Removed `{phrase}` from forbidden phrases.")
    else:
        await ctx.send(f"`{phrase}` was not in forbidden phrases.")

@bot.command(name="timeouts_add")
@commands.check(admin_or_power)
async def cmd_timeouts_add(ctx: commands.Context, *, phrase: str):
    """Add a timeout phrase"""
    gid = ctx.guild.id if ctx.guild else 0
    phrase = normalize_term(phrase)
    await bot.db.execute("INSERT OR IGNORE INTO timeout_phrases(guild_id, phrase) VALUES(?, ?)", (gid, phrase))
    await bot.db.commit()
    await ctx.send(f"✅ Added timeout phrase `{phrase}` for this server.")

@bot.command(name="timeouts_remove")
@commands.check(admin_or_power)
async def cmd_timeouts_remove(ctx: commands.Context, *, phrase: str):
    """Remove a timeout phrase"""
    gid = ctx.guild.id if ctx.guild else 0
    phrase = normalize_term(phrase)
    cur = await bot.db.execute("DELETE FROM timeout_phrases WHERE guild_id=? AND phrase=?", (gid, phrase))
    await bot.db.commit()
    if cur.rowcount:
        await ctx.send(f"🗑️ Removed `{phrase}` from timeout phrases.")
    else:
        await ctx.send(f"`{phrase}` was not in timeout phrases.")

@bot.command(name="response_set")
@commands.check(admin_or_power)
async def cmd_response_set(ctx: commands.Context, keyword: str, *, response: str):
    """Set a keyword response"""
    gid = ctx.guild.id if ctx.guild else 0
    keyword = normalize_term(keyword)
    await bot.db.execute(
        "INSERT OR REPLACE INTO keyword_responses(guild_id, keyword, response) VALUES(?,?,?)",
        (gid, keyword, response)
    )
    await bot.db.commit()
    await ctx.send(f"✅ Set response for `{keyword}`.")

@bot.command(name="response_unset")
@commands.check(admin_or_power)
async def cmd_response_unset(ctx: commands.Context, *, keyword: str):
    """Remove a keyword response"""
    gid = ctx.guild.id if ctx.guild else 0
    keyword = normalize_term(keyword)
    cur = await bot.db.execute("DELETE FROM keyword_responses WHERE guild_id=? AND keyword=?", (gid, keyword))
    await bot.db.commit()
    if cur.rowcount:
        await ctx.send(f"🗑️ Removed response for `{keyword}`.")
    else:
        await ctx.send(f"No response was set for `{keyword}`.")

@bot.command(name="export")
@commands.check(admin_or_power)
async def cmd_export(ctx: commands.Context):
    """Export all data for this server"""
    import json, tempfile, os
    gid = ctx.guild.id if ctx.guild else 0

    # tracked terms
    terms = []
    async with bot.db.execute("SELECT term FROM terms WHERE guild_id=? ORDER BY term", (gid,)) as cur:
        async for r in cur:
            terms.append(r[0])

    # term meta + user counts
    term_data = {}
    for term in terms:
        meta = await bot.db.execute_fetchone(
            "SELECT total_count, last_mentioned, last_user FROM term_meta WHERE guild_id=? AND term=?",
            (gid, term)
        )
        if meta:
            total, last_mentioned, last_user = meta
        else:
            total, last_mentioned, last_user = 0, None, None
        # per-user breakdown
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

    # forbidden/timeout/responses
    forbidden = []
    async with bot.db.execute("SELECT phrase FROM forbidden_phrases WHERE guild_id=? ORDER BY phrase", (gid,)) as cur:
        async for r in cur:
            forbidden.append(r[0])
    
    timeouts = []
    async with bot.db.execute("SELECT phrase FROM timeout_phrases WHERE guild_id=? ORDER BY phrase", (gid,)) as cur:
        async for r in cur:
            timeouts.append(r[0])
    
    responses = []
    async with bot.db.execute("SELECT keyword, response FROM keyword_responses WHERE guild_id=? ORDER BY keyword", (gid,)) as cur:
        async for k, v in cur:
            responses.append({"keyword": k, "response": v})

    # settings
    settings = await bot.get_guild_settings(gid)
    
    # ignored channels
    ignored_channels = []
    async with bot.db.execute("SELECT channel_id FROM ignored_channels WHERE guild_id=?", (gid,)) as cur:
        async for r in cur:
            ignored_channels.append(r[0])

    payload = {
        "guild_id": gid,
        "export_date": datetime.now(timezone.utc).isoformat(),
        "tracked_terms": terms,
        "term_data": term_data,
        "forbidden_phrases": forbidden,
        "timeout_phrases": timeouts,
        "keyword_responses": responses,
        "settings": settings,
        "ignored_channels": ignored_channels
    }

    guild_name = ctx.guild.name if ctx.guild else "DM"
    filename = f"termbot-export-{guild_name}-{gid}.json"
    
    with tempfile.NamedTemporaryFile("w", delete=False, suffix=".json") as fp:
        json.dump(payload, fp, indent=2)
        tmp_path = fp.name
    
    await ctx.send("📤 Server data export:", file=discord.File(tmp_path, filename=filename))
    os.unlink(tmp_path)

@bot.command(name="import")
@commands.check(admin_or_power)
async def cmd_import(ctx: commands.Context):
    """Import data from an export file"""
    import json
    gid = ctx.guild.id if ctx.guild else 0
    if not ctx.message.attachments:
        await ctx.send("❌ Attach a JSON export file to import.")
        return
    
    try:
        data_bytes = await ctx.message.attachments[0].read()
        data = json.loads(data_bytes.decode("utf-8"))
    except Exception as e:
        await ctx.send(f"❌ Couldn't parse JSON: {e}")
        return

    # Terms
    for term in (data.get("tracked_terms") or []):
        term = normalize_term(term)
        await bot.db.execute("INSERT OR IGNORE INTO terms(guild_id, term) VALUES(?, ?)", (gid, term))

    # Term data
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
        # user_counts can be a dict (old) or list of objects (new)
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

    # Forbidden/Timeout/Responses
    for p in (data.get("forbidden_phrases") or []):
        await bot.db.execute("INSERT OR IGNORE INTO forbidden_phrases(guild_id, phrase) VALUES(?, ?)", (gid, normalize_term(p)))
    for p in (data.get("timeout_phrases") or []):
        await bot.db.execute("INSERT OR IGNORE INTO timeout_phrases(guild_id, phrase) VALUES(?, ?)", (gid, normalize_term(p)))
    for kv in (data.get("keyword_responses") or []):
        k = normalize_term(kv.get("keyword", ""))
        v = str(kv.get("response", ""))
        if k:
            await bot.db.execute("INSERT OR REPLACE INTO keyword_responses(guild_id, keyword, response) VALUES(?, ?, ?)", (gid, k, v))

    # Settings
    settings_data = data.get("settings", {})
    if settings_data:
        await bot.db.execute(
            "INSERT OR REPLACE INTO guild_settings(guild_id, ignore_commands, case_sensitive, min_word_length, cooldown_seconds) VALUES(?,?,?,?,?)",
            (gid, settings_data.get("ignore_commands", True), settings_data.get("case_sensitive", False), 
             settings_data.get("min_word_length", 1), settings_data.get("cooldown_seconds", 0))
        )

    # Ignored channels (only if they still exist)
    if ctx.guild:
        for channel_id in (data.get("ignored_channels") or []):
            if ctx.guild.get_channel(channel_id):
                await bot.db.execute("INSERT OR IGNORE INTO ignored_channels(guild_id, channel_id) VALUES(?, ?)", (gid, channel_id))

    await bot.db.commit()
    await bot.refresh_patterns()
    await ctx.send("✅ Import complete for this server.")

@bot.command(name="help", aliases=["h"])
async def cmd_help(ctx: commands.Context, command: str = None):
    """Show help information"""
    if command:
        # Show help for specific command
        cmd_obj = bot.get_command(command)
        if not cmd_obj:
            await ctx.send(f"❌ Unknown command: `{command}`")
            return
        
        embed = discord.Embed(title=f"Help: `{COMMAND_PREFIX}{command}`", color=0x3498db)
        embed.description = cmd_obj.help or "No description available"
        
        if hasattr(cmd_obj, 'aliases') and cmd_obj.aliases:
            embed.add_field(name="Aliases", value=", ".join(f"`{a}`" for a in cmd_obj.aliases), inline=False)
        
        await ctx.send(embed=embed)
        return
    
    # General help
    embed = discord.Embed(title="🤖 Term Tracker Bot Help", color=0x3498db)
    embed.description = "Track and count mentions of specific terms across your server!"
    
    # Basic commands
    basic = [
        f"`{COMMAND_PREFIX}terms` - List tracked terms",
        f"`{COMMAND_PREFIX}stats [term]` - Show statistics",
        f"`{COMMAND_PREFIX}recent [term] [limit]` - Show recent messages",
        f"`{COMMAND_PREFIX}leaderboard [timeframe]` - User rankings",
        f"`{COMMAND_PREFIX}search <query>` - Search messages"
    ]
    embed.add_field(name="📊 Basic Commands", value="\n".join(basic), inline=False)
    
    # Admin commands
    admin = [
        f"`{COMMAND_PREFIX}track <term>` - Start tracking a term",
        f"`{COMMAND_PREFIX}untrack <term>` - Stop tracking a term",
        f"`{COMMAND_PREFIX}settings` - Show guild settings",
        f"`{COMMAND_PREFIX}set <setting> <value>` - Change settings",
        f"`{COMMAND_PREFIX}ignore_channel [channel]` - Ignore channel",
        f"`{COMMAND_PREFIX}reset [term]` - Reset statistics"
    ]
    embed.add_field(name="⚙️ Admin Commands", value="\n".join(admin), inline=False)
    
    embed.add_field(name="💡 Tips", 
                    value="• Use word boundaries for exact matches\n• Set cooldowns to prevent spam\n• Ignore command channels to avoid false positives", 
                    inline=False)
    
    embed.set_footer(text=f"Use {COMMAND_PREFIX}help <command> for detailed help")
    await ctx.send(embed=embed)

# Power user commands
def check_power(ctx: commands.Context) -> bool:
    return is_power_user(ctx.author)

@bot.command(name="pu_guilds")
@commands.check(check_power)
async def cmd_pu_guilds(ctx: commands.Context):
    """[Power User] List all guilds the bot is in"""
    lines = [f"{g.name} — {g.id} ({g.member_count} members)" for g in bot.guilds]
    if not lines:
        await ctx.send("Bot is not in any guilds.")
    else:
        embed = discord.Embed(title="🏰 Bot Guilds", color=0xe67e22)
        embed.description = "\n".join(lines[:50])
        if len(lines) > 50:
            embed.set_footer(text=f"Showing first 50 of {len(lines)} guilds")
        await ctx.send(embed=embed)

@bot.command(name="pu_all_terms")
@commands.check(check_power)
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
    
    embed = discord.Embed(title="🌍 Global Top Terms", color=0x9b59b6)
    lines = [f"{i+1}. `{t}` — {c:,}" for i, (t, c) in enumerate(rows)]
    embed.description = "\n".join(lines)
    await ctx.send(embed=embed)

@bot.command(name="pu_all_stats")
@commands.check(check_power)
async def cmd_pu_all_stats(ctx: commands.Context, term: str, limit: int = 20):
    """[Power User] Show global user stats for a term"""
    term = normalize_term(term)
    limit = max(1, min(100, limit))
    rows = []
    async with bot.db.execute(
        "SELECT user_name, SUM(count) AS total FROM hits WHERE term=? GROUP BY user_id, user_name ORDER BY total DESC LIMIT ?",
        (term, limit)
    ) as cur:
        async for r in cur:
            rows.append(r)
    if not rows:
        await ctx.send(f"No data yet for `{term}` across all guilds.")
        return
    
    embed = discord.Embed(title=f"🌍 Global Users for `{term}`", color=0x9b59b6)
    lines = [f"{i+1}. **{user}** — {c:,}" for i, (user, c) in enumerate(rows)]
    embed.description = "\n".join(lines)
    await ctx.send(embed=embed)

# Error handling
@bot.event
async def on_command_error(ctx: commands.Context, error):
    if isinstance(error, commands.CommandNotFound):
        return  # Ignore unknown commands
    elif isinstance(error, commands.CheckFailure):
        await ctx.send("❌ You don't have permission to use this command.")
    elif isinstance(error, commands.MissingRequiredArgument):
        await ctx.send(f"❌ Missing required argument. Use `{COMMAND_PREFIX}help {ctx.command.name}` for usage.")
    elif isinstance(error, commands.BadArgument):
        await ctx.send(f"❌ Invalid argument. Use `{COMMAND_PREFIX}help {ctx.command.name}` for usage.")
    else:
        log.error("Command error in %s: %s", ctx.command, error, exc_info=error)
        await ctx.send("❌ An error occurred while processing your command.")

# -------------------------
# Entrypoint
# -------------------------
def main():
    token = os.getenv("DISCORD_TOKEN")
    if not token:
        raise SystemExit("DISCORD_TOKEN not set in .env")
    # SSL workaround for some hosts
    try:
        import certifi
        os.environ.setdefault("SSL_CERT_FILE", certifi.where())
        os.environ.setdefault("REQUESTS_CA_BUNDLE", certifi.where())
    except Exception:
        pass
    bot.run(token, reconnect=True)

if __name__ == "__main__":
    main()