import os
import re
import json
import logging
import asyncio
from datetime import datetime, timedelta, timezone, time
from typing import List, Tuple, Dict, Optional

import discord
from discord.ext import commands, tasks
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
intents.members = True  # Enable for better user tracking

# -------------------------
# Enhanced Database schema
# -------------------------
SCHEMA = '''
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS terms (
    guild_id INTEGER NOT NULL,
    term TEXT NOT NULL,
    created_by INTEGER,
    created_at TEXT,
    PRIMARY KEY (guild_id, term)
);
CREATE TABLE IF NOT EXISTS term_meta (
    guild_id INTEGER NOT NULL,
    term TEXT NOT NULL,
    total_count INTEGER NOT NULL DEFAULT 0,
    last_mentioned TEXT,
    last_user TEXT,
    weekly_count INTEGER DEFAULT 0,
    monthly_count INTEGER DEFAULT 0,
    PRIMARY KEY (guild_id, term)
);
CREATE TABLE IF NOT EXISTS hits (
    guild_id INTEGER NOT NULL,
    term TEXT NOT NULL,
    user_id INTEGER NOT NULL,
    user_name TEXT NOT NULL,
    count INTEGER NOT NULL DEFAULT 0,
    last_seen TEXT,
    weekly_count INTEGER DEFAULT 0,
    monthly_count INTEGER DEFAULT 0,
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
    action TEXT DEFAULT 'warn',
    PRIMARY KEY (guild_id, phrase)
);
CREATE TABLE IF NOT EXISTS timeout_phrases (
    guild_id INTEGER NOT NULL,
    phrase TEXT NOT NULL,
    duration INTEGER DEFAULT 300,
    PRIMARY KEY (guild_id, phrase)
);
CREATE TABLE IF NOT EXISTS keyword_responses (
    guild_id INTEGER NOT NULL,
    keyword TEXT NOT NULL,
    response TEXT NOT NULL,
    response_type TEXT DEFAULT 'message',
    PRIMARY KEY (guild_id, keyword)
);
-- Enhanced ignored channels
CREATE TABLE IF NOT EXISTS ignored_channels (
    guild_id INTEGER NOT NULL,
    channel_id INTEGER NOT NULL,
    ignored_by INTEGER,
    ignored_at TEXT,
    PRIMARY KEY (guild_id, channel_id)
);
-- Enhanced guild settings
CREATE TABLE IF NOT EXISTS guild_settings (
    guild_id INTEGER NOT NULL PRIMARY KEY,
    ignore_commands BOOLEAN DEFAULT true,
    case_sensitive BOOLEAN DEFAULT false,
    min_word_length INTEGER DEFAULT 1,
    cooldown_seconds INTEGER DEFAULT 0,
    auto_cleanup_days INTEGER DEFAULT 0,
    notification_channel INTEGER,
    daily_summary BOOLEAN DEFAULT false,
    theme_color INTEGER DEFAULT 3447003
);
-- Enhanced user cooldowns
CREATE TABLE IF NOT EXISTS user_cooldowns (
    guild_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    term TEXT NOT NULL,
    last_increment TEXT,
    PRIMARY KEY (guild_id, user_id, term)
);
-- New: Term categories
CREATE TABLE IF NOT EXISTS term_categories (
    guild_id INTEGER NOT NULL,
    category_name TEXT NOT NULL,
    description TEXT,
    color INTEGER DEFAULT 3447003,
    PRIMARY KEY (guild_id, category_name)
);
CREATE TABLE IF NOT EXISTS term_category_assignments (
    guild_id INTEGER NOT NULL,
    term TEXT NOT NULL,
    category_name TEXT NOT NULL,
    PRIMARY KEY (guild_id, term),
    FOREIGN KEY (guild_id, term) REFERENCES terms(guild_id, term)
);
-- New: Achievement system
CREATE TABLE IF NOT EXISTS achievements (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    description TEXT NOT NULL,
    requirement_type TEXT NOT NULL,
    requirement_value INTEGER NOT NULL,
    badge_emoji TEXT DEFAULT '🏆'
);
CREATE TABLE IF NOT EXISTS user_achievements (
    guild_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    achievement_id INTEGER NOT NULL,
    earned_at TEXT NOT NULL,
    PRIMARY KEY (guild_id, user_id, achievement_id),
    FOREIGN KEY (achievement_id) REFERENCES achievements(id)
);
-- New: Daily/Weekly stats tracking
CREATE TABLE IF NOT EXISTS daily_stats (
    guild_id INTEGER NOT NULL,
    date TEXT NOT NULL,
    term TEXT NOT NULL,
    total_mentions INTEGER DEFAULT 0,
    unique_users INTEGER DEFAULT 0,
    PRIMARY KEY (guild_id, date, term)
);
-- New: Term aliases
CREATE TABLE IF NOT EXISTS term_aliases (
    guild_id INTEGER NOT NULL,
    alias TEXT NOT NULL,
    main_term TEXT NOT NULL,
    PRIMARY KEY (guild_id, alias),
    FOREIGN KEY (guild_id, main_term) REFERENCES terms(guild_id, term)
);
-- New: User preferences
CREATE TABLE IF NOT EXISTS user_preferences (
    guild_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    setting_name TEXT NOT NULL,
    setting_value TEXT NOT NULL,
    PRIMARY KEY (guild_id, user_id, setting_name)
);
'''

# Default achievements
DEFAULT_ACHIEVEMENTS = [
    ("First Steps", "Mention your first tracked term", "first_mention", 1, "👶"),
    ("Chatterbox", "Mention tracked terms 100 times", "total_mentions", 100, "💬"),
    ("Veteran", "Mention tracked terms 500 times", "total_mentions", 500, "⭐"),
    ("Legend", "Mention tracked terms 1000 times", "total_mentions", 1000, "🏆"),
    ("Diverse", "Mention 10 different terms", "unique_terms", 10, "🌈"),
    ("Specialist", "Get 50 mentions on a single term", "term_mentions", 50, "🎯"),
    ("Weekly Champion", "Top user for the week", "weekly_top", 1, "👑"),
    ("Monthly King", "Top user for the month", "monthly_top", 1, "🔥")
]

async def init_db(db: aiosqlite.Connection):
    for stmt in SCHEMA.strip().split(";"):
        s = stmt.strip()
        if s:
            await db.execute(s)
    
    # Insert default achievements
    for name, desc, req_type, req_val, emoji in DEFAULT_ACHIEVEMENTS:
        await db.execute(
            "INSERT OR IGNORE INTO achievements (name, description, requirement_type, requirement_value, badge_emoji) VALUES (?,?,?,?,?)",
            (name, desc, req_type, req_val, emoji)
        )
    
    await db.commit()

# -------------------------
# Enhanced Utilities
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

def create_progress_bar(current: int, target: int, length: int = 10) -> str:
    """Create a simple text progress bar"""
    if target == 0:
        return "▓" * length
    
    progress = min(current / target, 1.0)
    filled = int(progress * length)
    empty = length - filled
    return "▓" * filled + "░" * empty

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
# Migration (keeping existing logic)
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
    now = datetime.now(timezone.utc).isoformat()
    for term in tracked_terms or term_data.keys():
        norm = normalize_term(term)
        await db.execute("INSERT OR IGNORE INTO terms(guild_id, term, created_at) VALUES(0, ?, ?)", (norm, now))

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
                "INSERT OR REPLACE INTO hits(guild_id, term, user_id, user_name, count, last_seen) VALUES(0,?,?,?,?,?)",
                (norm, int(user), str(user), int(cnt or 0), last_mentioned)
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
# Enhanced Bot Class
# -------------------------
class TermBot(commands.Bot):
    def __init__(self):
        super().__init__(command_prefix=COMMAND_PREFIX, intents=intents, help_command=None)
        self.db: aiosqlite.Connection | None = None
        self.patterns: Dict[int, List[Tuple[str, re.Pattern]]] = {}
        self.aliases: Dict[int, Dict[str, str]] = {}  # guild_id -> {alias: main_term}

    def _gid(self, message: discord.Message) -> int:
        return message.guild.id if message.guild else 0

    async def setup_hook(self) -> None:
        self.db = await aiosqlite.connect(DB_PATH)
        await init_db(self.db)
        if await needs_migration(self.db):
            await migrate_json(self.db)
        await self.refresh_patterns()
        
        # Start background tasks
        self.cleanup_old_data.start()
        self.daily_summary_task.start()

    async def get_guild_settings(self, guild_id: int) -> Dict:
        """Get guild-specific settings"""
        async with self.db.execute(
            "SELECT ignore_commands, case_sensitive, min_word_length, cooldown_seconds, auto_cleanup_days, notification_channel, daily_summary, theme_color FROM guild_settings WHERE guild_id=?", 
            (guild_id,)
        ) as cur:
            row = await cur.fetchone()
        
        if row:
            return {
                'ignore_commands': bool(row[0]),
                'case_sensitive': bool(row[1]),
                'min_word_length': row[2],
                'cooldown_seconds': row[3],
                'auto_cleanup_days': row[4],
                'notification_channel': row[5],
                'daily_summary': bool(row[6]),
                'theme_color': row[7] or 3447003
            }
        else:
            # Return defaults
            return {
                'ignore_commands': True,
                'case_sensitive': False,
                'min_word_length': 1,
                'cooldown_seconds': 0,
                'auto_cleanup_days': 0,
                'notification_channel': None,
                'daily_summary': False,
                'theme_color': 3447003
            }

    async def resolve_term(self, guild_id: int, term: str) -> str:
        """Resolve term alias to main term"""
        term = normalize_term(term)
        return self.aliases.get(guild_id, {}).get(term, term)

    async def refresh_patterns(self):
        """Refresh regex patterns and aliases for all guilds"""
        self.patterns.clear()
        self.aliases.clear()
        
        # Load aliases
        async with self.db.execute("SELECT guild_id, alias, main_term FROM term_aliases") as cur:
            async for gid, alias, main_term in cur:
                if gid not in self.aliases:
                    self.aliases[gid] = {}
                self.aliases[gid][alias] = main_term
        
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
            # Add aliases to patterns
            if gid in self.aliases:
                filtered_terms.extend(self.aliases[gid].keys())
            self.patterns[gid] = build_patterns(filtered_terms, settings['case_sensitive'])

    async def check_achievements(self, guild_id: int, user_id: int):
        """Check and award achievements for user"""
        # Get user's current stats
        async with self.db.execute(
            "SELECT COUNT(DISTINCT term), SUM(count) FROM hits WHERE guild_id=? AND user_id=?",
            (guild_id, user_id)
        ) as cur:
            unique_terms, total_mentions = await cur.fetchone()
        
        unique_terms = unique_terms or 0
        total_mentions = total_mentions or 0
        
        # Check for achievements
        async with self.db.execute("SELECT id, name, requirement_type, requirement_value, badge_emoji FROM achievements") as cur:
            achievements = await cur.fetchall()
        
        new_achievements = []
        for ach_id, name, req_type, req_value, emoji in achievements:
            # Check if user already has this achievement
            async with self.db.execute(
                "SELECT 1 FROM user_achievements WHERE guild_id=? AND user_id=? AND achievement_id=?",
                (guild_id, user_id, ach_id)
            ) as cur:
                if await cur.fetchone():
                    continue
            
            earned = False
            if req_type == "total_mentions" and total_mentions >= req_value:
                earned = True
            elif req_type == "unique_terms" and unique_terms >= req_value:
                earned = True
            elif req_type == "first_mention" and total_mentions >= 1:
                earned = True
            elif req_type == "term_mentions":
                # Check if user has req_value mentions on any single term
                async with self.db.execute(
                    "SELECT MAX(count) FROM hits WHERE guild_id=? AND user_id=?",
                    (guild_id, user_id)
                ) as cur:
                    max_count = (await cur.fetchone())[0] or 0
                    if max_count >= req_value:
                        earned = True
            
            if earned:
                now = datetime.now(timezone.utc).isoformat()
                await self.db.execute(
                    "INSERT INTO user_achievements(guild_id, user_id, achievement_id, earned_at) VALUES(?,?,?,?)",
                    (guild_id, user_id, ach_id, now)
                )
                new_achievements.append((name, emoji))
        
        if new_achievements:
            await self.db.commit()
        
        return new_achievements

    @tasks.loop(hours=24)
    async def cleanup_old_data(self):
        """Clean up old data based on guild settings"""
        async with self.db.execute("SELECT guild_id, auto_cleanup_days FROM guild_settings WHERE auto_cleanup_days > 0") as cur:
            async for guild_id, days in cur:
                cutoff = datetime.now(timezone.utc) - timedelta(days=days)
                cutoff_str = cutoff.isoformat()
                
                # Delete old messages
                await self.db.execute(
                    "DELETE FROM messages WHERE guild_id=? AND created_at < ?",
                    (guild_id, cutoff_str)
                )
        
        await self.db.commit()

    @tasks.loop(time=time(hour=9, minute=0, tzinfo=timezone.utc))  # 9 AM UTC daily
    async def daily_summary_task(self):
        """Send daily summaries to guilds that have it enabled"""
        yesterday = datetime.now(timezone.utc) - timedelta(days=1)
        yesterday_str = yesterday.strftime("%Y-%m-%d")
        
        async with self.db.execute(
            "SELECT guild_id, notification_channel, theme_color FROM guild_settings WHERE daily_summary=1 AND notification_channel IS NOT NULL"
        ) as cur:
            async for guild_id, channel_id, theme_color in cur:
                guild = self.get_guild(guild_id)
                if not guild:
                    continue
                    
                channel = guild.get_channel(channel_id)
                if not channel:
                    continue
                
                # Get yesterday's stats
                async with self.db.execute(
                    "SELECT term, COUNT(*) as mentions, COUNT(DISTINCT user_id) as users FROM messages WHERE guild_id=? AND DATE(created_at) = ? GROUP BY term ORDER BY mentions DESC LIMIT 5",
                    (guild_id, yesterday_str)
                ) as stats_cur:
                    top_terms = await stats_cur.fetchall()
                
                if not top_terms:
                    continue
                
                embed = discord.Embed(
                    title=f"Daily Summary - {yesterday.strftime('%B %d, %Y')}",
                    color=theme_color,
                    timestamp=yesterday
                )
                
                summary = "\n".join([f"**{term}** - {mentions} mentions by {users} users" for term, mentions, users in top_terms])
                embed.add_field(name="Top Terms", value=summary, inline=False)
                
                try:
                    await channel.send(embed=embed)
                except discord.HTTPException:
                    pass  # Channel might not be accessible

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

    async def increment(self, message: discord.Message, term: str, occurrences: int = 1):
        """Enhanced increment with weekly/monthly tracking and achievements"""
        gid = self._gid(message)
        original_term = term
        term = await self.resolve_term(gid, normalize_term(term))
        now = datetime.now(timezone.utc).isoformat()
        user_id = int(message.author.id)
        user_name = str(message.author)

        # Update main counters
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

        # Update daily stats
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        await self.db.execute(
            "INSERT INTO daily_stats(guild_id, date, term, total_mentions, unique_users) VALUES(?,?,?,?,1) "
            "ON CONFLICT(guild_id, date, term) DO UPDATE SET "
            "total_mentions = daily_stats.total_mentions + excluded.total_mentions",
            (gid, today, term, occurrences)
        )

        # Check for achievements (async, don't await to avoid slowing down message processing)
        asyncio.create_task(self.check_achievements(gid, user_id))

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
                # Clean up all tables
                tables = ["terms", "term_meta", "hits", "messages", "guild_settings", 
                         "ignored_channels", "user_cooldowns", "forbidden_phrases", 
                         "timeout_phrases", "keyword_responses", "term_categories",
                         "term_category_assignments", "user_achievements", "daily_stats",
                         "term_aliases", "user_preferences"]
                
                for table in tables:
                    await self.db.execute(f"DELETE FROM {table} WHERE guild_id = ?", (gid,))
            
            await self.db.commit()
            await self.refresh_patterns()

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
                resolved_term = await self.resolve_term(gid, term)
                if not await self.check_cooldown(gid, message.author.id, resolved_term, settings['cooldown_seconds']):
                    count = len(matches)
                    await self.increment(message, term, occurrences=count)
                    await self.update_cooldown(gid, message.author.id, resolved_term)
                    matched_terms.append((resolved_term, count))

        if matched_terms:
            await self.db.commit()
            log.debug("Matched terms in message %s: %s", message.id, matched_terms)

        await self.process_commands(message)

bot = TermBot()

# -------------------------
# Enhanced Commands
# -------------------------

@bot.command(name="terms")
async def cmd_terms(ctx: commands.Context, category: str = None):
    """List all tracked terms, optionally filtered by category"""
    gid = ctx.guild.id if ctx.guild else 0
    settings = await bot.get_guild_settings(gid)
    
    if category:
        # Show terms in specific category
        category = normalize_term(category)
        async with bot.db.execute(
            """SELECT t.term, tm.total_count FROM terms t 
               LEFT JOIN term_meta tm ON t.guild_id = tm.guild_id AND t.term = tm.term
               JOIN term_category_assignments tca ON t.guild_id = tca.guild_id AND t.term = tca.term
               WHERE t.guild_id=? AND tca.category_name=? ORDER BY tm.total_count DESC""", 
            (gid, category)
        ) as cur:
            terms = await cur.fetchall()
            
        if not terms:
            await ctx.send(f"No terms found in category `{category}`.")
            return
            
        embed = discord.Embed(
            title=f"Terms in category: {category}",
            color=settings['theme_color']
        )
    else:
        # Show all terms with their categories
        async with bot.db.execute(
            """SELECT t.term, tm.total_count, tca.category_name FROM terms t 
               LEFT JOIN term_meta tm ON t.guild_id = tm.guild_id AND t.term = tm.term
               LEFT JOIN term_category_assignments tca ON t.guild_id = tca.guild_id AND t.term = tca.term
               WHERE t.guild_id=? ORDER BY tm.total_count DESC""", 
            (gid,)
        ) as cur:
            terms = await cur.fetchall()
            
        if not terms:
            await ctx.send("No tracked terms yet. Use `!track <term>` to add one.")
            return
            
        embed = discord.Embed(
            title="Tracked Terms",
            color=settings['theme_color']
        )
    
    # Format terms with counts and categories
    terms_text = []
    for term, count, cat in terms:
        count_str = f" ({count or 0})" if count else ""
        cat_str = f" [{cat}]" if cat else ""
        terms_text.append(f"`{term}`{count_str}{cat_str}")
    
    # Split into chunks if too long
    terms_str = ", ".join(terms_text)
    if len(terms_str) > 1900:
        chunks = []
        current_chunk = ""
        for term_str in terms_text:
            if len(current_chunk + term_str + ", ") > 1900:
                chunks.append(current_chunk.rstrip(", "))
                current_chunk = term_str + ", "
            else:
                current_chunk += term_str + ", "
        if current_chunk.strip():
            chunks.append(current_chunk.rstrip(", "))
        
        for i, chunk in enumerate(chunks):
            if i == 0:
                embed.description = chunk
                await ctx.send(embed=embed)
            else:
                await ctx.send(chunk)
    else:
        embed.description = terms_str
        await ctx.send(embed=embed)

@bot.command(name="track")
@commands.check(admin_or_power)
async def cmd_track(ctx: commands.Context, *, term: str):
    """Add a term to track in this server"""
    gid = ctx.guild.id if ctx.guild else 0
    term = normalize_term(term)
    settings = await bot.get_guild_settings(gid)
    
    if len(term) == 0:
        await ctx.send("❌ Term cannot be empty.")
        return
    
    if len(term) < settings['min_word_length']:
        await ctx.send(f"❌ Term must be at least {settings['min_word_length']} characters long.")
        return
    
    # Check if already tracking
    async with bot.db.execute("SELECT 1 FROM terms WHERE guild_id=? AND term=?", (gid, term)) as cur:
        exists = await cur.fetchone()
    
    if exists:
        await ctx.send(f"⚠️ Already tracking `{term}`.")
        return
    
    now = datetime.now(timezone.utc).isoformat()
    await bot.db.execute("INSERT OR IGNORE INTO terms(guild_id, term, created_by, created_at) VALUES(?, ?, ?, ?)", 
                         (gid, term, ctx.author.id, now))
    await bot.db.commit()
    await bot.refresh_patterns()
    
    embed = discord.Embed(
        title="Term Added",
        description=f"Now tracking `{term}`",
        color=settings['theme_color']
    )
    embed.set_footer(text=f"Added by {ctx.author.display_name}")
    await ctx.send(embed=embed)

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
    
    # Delete from all related tables
    tables = ["terms", "term_meta", "hits", "messages", "user_cooldowns", 
              "term_category_assignments", "term_aliases", "daily_stats"]
    
    for table in tables:
        await bot.db.execute(f"DELETE FROM {table} WHERE guild_id=? AND term=?", (gid, term))
    
    await bot.db.commit()
    await bot.refresh_patterns()
    
    settings = await bot.get_guild_settings(gid)
    embed = discord.Embed(
        title="Term Removed",
        description=f"Removed `{term}` from tracked terms",
        color=settings['theme_color']
    )
    embed.add_field(name="Data Deleted", value=f"{total_count} total mentions", inline=False)
    await ctx.send(embed=embed)

@bot.command(name="stats")
async def cmd_stats(ctx: commands.Context, *, term: str = None):
    """Show enhanced statistics for a term or all terms"""
    gid = ctx.guild.id if ctx.guild else 0
    settings = await bot.get_guild_settings(gid)
    
    if term:
        term = await bot.resolve_term(gid, normalize_term(term))
        
        # Get comprehensive stats
        async with bot.db.execute(
            "SELECT total_count, last_mentioned, last_user FROM term_meta WHERE guild_id=? AND term=?", 
            (gid, term)
        ) as cur:
            meta_row = await cur.fetchone()
            
        if not meta_row:
            await ctx.send(f"No data yet for `{term}`.")
            return
            
        total_count, last_mentioned, last_user = meta_row
        
        # Get weekly and daily stats
        week_ago = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
        day_ago = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
        
        async with bot.db.execute(
            "SELECT COUNT(*) FROM messages WHERE guild_id=? AND term=? AND created_at >= ?",
            (gid, term, week_ago)
        ) as cur:
            weekly_count = (await cur.fetchone())[0]
            
        async with bot.db.execute(
            "SELECT COUNT(*) FROM messages WHERE guild_id=? AND term=? AND created_at >= ?",
            (gid, term, day_ago)
        ) as cur:
            daily_count = (await cur.fetchone())[0]
        
        # Get top users
        async with bot.db.execute(
            "SELECT user_name, count, last_seen FROM hits WHERE guild_id=? AND term=? ORDER BY count DESC LIMIT 10", 
            (gid, term)
        ) as cur:
            top_users = await cur.fetchall()
        
        embed = discord.Embed(
            title=f"📊 Stats for `{term}`", 
            color=settings['theme_color']
        )
        
        # Main stats
        embed.add_field(name="Total Mentions", value=f"{total_count:,}", inline=True)
        embed.add_field(name="This Week", value=f"{weekly_count:,}", inline=True)
        embed.add_field(name="Today", value=f"{daily_count:,}", inline=True)
        
        if last_mentioned and last_user:
            try:
                last_time = datetime.fromisoformat(last_mentioned.replace('Z', '+00:00'))
                embed.add_field(name="Last Mentioned", 
                              value=f"<t:{int(last_time.timestamp())}:R> by {last_user}", 
                              inline=False)
            except:
                embed.add_field(name="Last Mentioned", value=f"by {last_user}", inline=False)
        
        if top_users:
            top_list = "\n".join([f"{i+1}. **{user}** — {count:,}" for i, (user, count, _) in enumerate(top_users[:5])])
            embed.add_field(name="Top Users", value=top_list, inline=False)
        
        await ctx.send(embed=embed)
    else:
        # Show overview of all terms
        async with bot.db.execute(
            "SELECT term, total_count FROM term_meta WHERE guild_id=? ORDER BY total_count DESC LIMIT 15", 
            (gid,)
        ) as cur:
            rows = await cur.fetchall()
                
        if not rows:
            await ctx.send("No data yet.")
            return
            
        embed = discord.Embed(title="📊 Top Terms", color=settings['theme_color'])
        
        # Create a nice chart-like display
        top_terms = []
        max_count = rows[0][1] if rows else 1
        
        for i, (term, count) in enumerate(rows):
            bar_length = int((count / max_count) * 20) if max_count > 0 else 0
            bar = "▓" * bar_length + "░" * (20 - bar_length)
            top_terms.append(f"{i+1:2d}. `{term}` {bar} {count:,}")
        
        embed.description = "\n".join(top_terms)
        await ctx.send(embed=embed)

@bot.command(name="achievements", aliases=["ach"])
async def cmd_achievements(ctx: commands.Context, user: discord.Member = None):
    """Show achievements for yourself or another user"""
    gid = ctx.guild.id if ctx.guild else 0
    target_user = user or ctx.author
    settings = await bot.get_guild_settings(gid)
    
    # Get user's achievements
    async with bot.db.execute(
        """SELECT a.name, a.description, a.badge_emoji, ua.earned_at 
           FROM user_achievements ua 
           JOIN achievements a ON ua.achievement_id = a.id 
           WHERE ua.guild_id=? AND ua.user_id=? 
           ORDER BY ua.earned_at DESC""",
        (gid, target_user.id)
    ) as cur:
        achievements = await cur.fetchall()
    
    embed = discord.Embed(
        title=f"🏆 {target_user.display_name}'s Achievements",
        color=settings['theme_color']
    )
    
    if achievements:
        ach_list = []
        for name, desc, emoji, earned_at in achievements:
            try:
                timestamp = datetime.fromisoformat(earned_at)
                time_str = f"<t:{int(timestamp.timestamp())}:R>"
            except:
                time_str = "recently"
            ach_list.append(f"{emoji} **{name}** — {desc}\n*Earned {time_str}*")
        
        embed.description = "\n\n".join(ach_list)
    else:
        embed.description = "No achievements yet! Start mentioning tracked terms to earn some."
    
    # Show progress towards next achievements
    async with bot.db.execute(
        "SELECT COUNT(DISTINCT term), SUM(count) FROM hits WHERE guild_id=? AND user_id=?",
        (gid, target_user.id)
    ) as cur:
        unique_terms, total_mentions = await cur.fetchone()
    
    unique_terms = unique_terms or 0
    total_mentions = total_mentions or 0
    
    if total_mentions > 0:
        progress = f"📈 **Progress**: {total_mentions:,} mentions across {unique_terms} terms"
        embed.add_field(name="Current Stats", value=progress, inline=False)
    
    await ctx.send(embed=embed)

@bot.command(name="category")
@commands.check(admin_or_power)
async def cmd_category(ctx: commands.Context, action: str = None, *, args: str = None):
    """Manage term categories: create, assign, list"""
    gid = ctx.guild.id if ctx.guild else 0
    settings = await bot.get_guild_settings(gid)
    
    if not action:
        # List all categories
        async with bot.db.execute(
            "SELECT category_name, description, COUNT(tca.term) as term_count FROM term_categories tc "
            "LEFT JOIN term_category_assignments tca ON tc.guild_id = tca.guild_id AND tc.category_name = tca.category_name "
            "WHERE tc.guild_id=? GROUP BY tc.category_name ORDER BY tc.category_name", 
            (gid,)
        ) as cur:
            categories = await cur.fetchall()
        
        if not categories:
            await ctx.send("No categories created yet. Use `!category create <name> [description]`")
            return
            
        embed = discord.Embed(title="📂 Term Categories", color=settings['theme_color'])
        cat_list = []
        for name, desc, count in categories:
            desc_text = f" - {desc}" if desc else ""
            cat_list.append(f"**{name}** ({count} terms){desc_text}")
        embed.description = "\n".join(cat_list)
        await ctx.send(embed=embed)
        
    elif action.lower() == "create":
        if not args:
            await ctx.send("Usage: `!category create <name> [description]`")
            return
            
        parts = args.split(" ", 1)
        name = normalize_term(parts[0])
        description = parts[1] if len(parts) > 1 else None
        
        await bot.db.execute(
            "INSERT OR IGNORE INTO term_categories(guild_id, category_name, description) VALUES(?,?,?)",
            (gid, name, description)
        )
        await bot.db.commit()
        
        embed = discord.Embed(
            title="Category Created",
            description=f"Created category `{name}`" + (f": {description}" if description else ""),
            color=settings['theme_color']
        )
        await ctx.send(embed=embed)
        
    elif action.lower() == "assign":
        if not args:
            await ctx.send("Usage: `!category assign <term> <category>`")
            return
            
        parts = args.split(" ", 1)
        if len(parts) < 2:
            await ctx.send("Usage: `!category assign <term> <category>`")
            return
            
        term = normalize_term(parts[0])
        category = normalize_term(parts[1])
        
        # Check if term exists
        async with bot.db.execute("SELECT 1 FROM terms WHERE guild_id=? AND term=?", (gid, term)) as cur:
            if not await cur.fetchone():
                await ctx.send(f"❌ Term `{term}` is not being tracked.")
                return
        
        # Check if category exists
        async with bot.db.execute("SELECT 1 FROM term_categories WHERE guild_id=? AND category_name=?", (gid, category)) as cur:
            if not await cur.fetchone():
                await ctx.send(f"❌ Category `{category}` does not exist.")
                return
        
        await bot.db.execute(
            "INSERT OR REPLACE INTO term_category_assignments(guild_id, term, category_name) VALUES(?,?,?)",
            (gid, term, category)
        )
        await bot.db.commit()
        
        await ctx.send(f"✅ Assigned `{term}` to category `{category}`.")
        
    else:
        await ctx.send("Unknown action. Use: create, assign, or no action to list categories.")

@bot.command(name="alias")
@commands.check(admin_or_power)
async def cmd_alias(ctx: commands.Context, alias: str = None, *, main_term: str = None):
    """Create aliases for terms (e.g., !alias lol 'laugh out loud')"""
    gid = ctx.guild.id if ctx.guild else 0
    
    if not alias:
        # List all aliases
        async with bot.db.execute(
            "SELECT alias, main_term FROM term_aliases WHERE guild_id=? ORDER BY alias",
            (gid,)
        ) as cur:
            aliases = await cur.fetchall()
        
        if not aliases:
            await ctx.send("No aliases created yet. Use `!alias <alias> <main_term>`")
            return
            
        settings = await bot.get_guild_settings(gid)
        embed = discord.Embed(title="🔗 Term Aliases", color=settings['theme_color'])
        alias_list = [f"`{alias_name}` → `{main}`" for alias_name, main in aliases]
        embed.description = "\n".join(alias_list)
        await ctx.send(embed=embed)
        return
    
    if not main_term:
        await ctx.send("Usage: `!alias <alias> <main_term>`")
        return
    
    alias = normalize_term(alias)
    main_term = normalize_term(main_term)
    
    # Check if main term exists
    async with bot.db.execute("SELECT 1 FROM terms WHERE guild_id=? AND term=?", (gid, main_term)) as cur:
        if not await cur.fetchone():
            await ctx.send(f"❌ Main term `{main_term}` is not being tracked.")
            return
    
    await bot.db.execute(
        "INSERT OR REPLACE INTO term_aliases(guild_id, alias, main_term) VALUES(?,?,?)",
        (gid, alias, main_term)
    )
    await bot.db.commit()
    await bot.refresh_patterns()
    
    await ctx.send(f"✅ Created alias `{alias}` → `{main_term}`")

@bot.command(name="dashboard")
async def cmd_dashboard(ctx: commands.Context):
    """Show a comprehensive dashboard of server statistics"""
    gid = ctx.guild.id if ctx.guild else 0
    settings = await bot.get_guild_settings(gid)
    
    # Get overall stats
    async with bot.db.execute(
        "SELECT COUNT(DISTINCT term), COALESCE(SUM(total_count), 0) FROM term_meta WHERE guild_id=?",
        (gid,)
    ) as cur:
        total_terms, total_mentions = await cur.fetchone()
    
    async with bot.db.execute(
        "SELECT COUNT(DISTINCT user_id) FROM hits WHERE guild_id=?",
        (gid,)
    ) as cur:
        active_users = (await cur.fetchone())[0] or 0
    
    # Get today's stats
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    async with bot.db.execute(
        "SELECT COUNT(*) FROM messages WHERE guild_id=? AND DATE(created_at) = ?",
        (gid, today)
    ) as cur:
        today_mentions = (await cur.fetchone())[0] or 0
    
    # Get top term today
    async with bot.db.execute(
        "SELECT term, COUNT(*) as count FROM messages WHERE guild_id=? AND DATE(created_at) = ? GROUP BY term ORDER BY count DESC LIMIT 1",
        (gid, today)
    ) as cur:
        top_today = await cur.fetchone()
    
    embed = discord.Embed(
        title=f"📊 {ctx.guild.name} Dashboard",
        color=settings['theme_color']
    )
    
    # Overview section
    overview = f"**Terms Tracked**: {total_terms:,}\n"
    overview += f"**Total Mentions**: {total_mentions:,}\n"
    overview += f"**Active Users**: {active_users:,}\n"
    overview += f"**Today**: {today_mentions:,} mentions"
    
    if top_today:
        overview += f"\n**Trending**: `{top_today[0]}` ({top_today[1]} mentions)"
    
    embed.add_field(name="📈 Overview", value=overview, inline=True)
    
    # Recent activity
    async with bot.db.execute(
        "SELECT term, COUNT(*) as mentions FROM messages WHERE guild_id=? AND created_at >= ? GROUP BY term ORDER BY mentions DESC LIMIT 5",
        (gid, (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat())
    ) as cur:
        recent_activity = await cur.fetchall()
    
    if recent_activity:
        activity_text = "\n".join([f"`{term}`: {count}" for term, count in recent_activity])
        embed.add_field(name="🔥 Last 24 Hours", value=activity_text, inline=True)
    
    # Top users this week
    week_ago = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
    async with bot.db.execute(
        "SELECT user_name, COUNT(*) as mentions FROM messages WHERE guild_id=? AND created_at >= ? GROUP BY user_id, user_name ORDER BY mentions DESC LIMIT 5",
        (gid, week_ago)
    ) as cur:
        weekly_leaders = await cur.fetchall()
    
    if weekly_leaders:
        leaders_text = "\n".join([f"**{user}**: {mentions}" for user, mentions in weekly_leaders])
        embed.add_field(name="👑 Weekly Leaders", value=leaders_text, inline=True)
    
    await ctx.send(embed=embed)

@bot.command(name="trends")
async def cmd_trends(ctx: commands.Context, days: int = 7):
    """Show trending terms over the specified number of days"""
    gid = ctx.guild.id if ctx.guild else 0
    settings = await bot.get_guild_settings(gid)
    days = max(1, min(90, days))  # Limit between 1 and 90 days
    
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    
    async with bot.db.execute(
        """SELECT term, COUNT(*) as recent_mentions,
                  COALESCE(tm.total_count, 0) as total_mentions
           FROM messages m
           LEFT JOIN term_meta tm ON m.guild_id = tm.guild_id AND m.term = tm.term
           WHERE m.guild_id=? AND m.created_at >= ?
           GROUP BY m.term
           ORDER BY recent_mentions DESC
           LIMIT 10""",
        (gid, cutoff)
    ) as cur:
        trends = await cur.fetchall()
    
    if not trends:
        await ctx.send(f"No activity in the last {days} day(s).")
        return
    
    embed = discord.Embed(
        title=f"📈 Trending Terms ({days} day{'s' if days > 1 else ''})",
        color=settings['theme_color']
    )
    
    trend_list = []
    max_recent = trends[0][1] if trends else 1
    
    for i, (term, recent, total) in enumerate(trends):
        # Create a simple trend indicator
        trend_bar = "▓" * int((recent / max_recent) * 10) if max_recent > 0 else ""
        trend_list.append(f"{i+1:2d}. `{term}` {trend_bar} {recent:,} ({total:,} total)")
    
    embed.description = "\n".join(trend_list)
    embed.set_footer(text=f"Showing activity from last {days} day{'s' if days > 1 else ''}")
    
    await ctx.send(embed=embed)

# Enhanced settings and utility commands
@bot.command(name="settings")
@commands.check(admin_or_power)
async def cmd_settings(ctx: commands.Context):
    """Show current guild settings with enhanced display"""
    gid = ctx.guild.id if ctx.guild else 0
    settings = await bot.get_guild_settings(gid)
    
    embed = discord.Embed(title="⚙️ Guild Settings", color=settings['theme_color'])
    
    # Basic settings
    basic = []
    basic.append(f"**Ignore Commands**: {'✅ Yes' if settings['ignore_commands'] else '❌ No'}")
    basic.append(f"**Case Sensitive**: {'✅ Yes' if settings['case_sensitive'] else '❌ No'}")
    basic.append(f"**Min Word Length**: {settings['min_word_length']}")
    basic.append(f"**Cooldown**: {format_duration(settings['cooldown_seconds']) if settings['cooldown_seconds'] > 0 else 'Disabled'}")
    embed.add_field(name="📝 Basic Settings", value="\n".join(basic), inline=True)
    
    # Advanced settings
    advanced = []
    days = int(settings['auto_cleanup_days'] or 0)
    advanced.append(
        f"**Auto Cleanup**: {'Disabled' if days <= 0 else str(days) + ' days'}"
    )
    advanced.append(f"**Daily Summary**: {'✅ Enabled' if settings['daily_summary'] else '❌ Disabled'}")
    
    if settings['notification_channel']:
        channel = ctx.guild.get_channel(settings['notification_channel'])
        channel_name = f"#{channel.name}" if channel else f"Unknown ({settings['notification_channel']})"
        advanced.append(f"**Notification Channel**: {channel_name}")
    else:
        advanced.append("**Notification Channel**: Not set")
    
    embed.add_field(name="🔧 Advanced Settings", value="\n".join(advanced), inline=True)
    
    # Show ignored channels
    async with bot.db.execute("SELECT channel_id FROM ignored_channels WHERE guild_id=?", (gid,)) as cur:
        ignored = []
        async for row in cur:
            channel = ctx.guild.get_channel(row[0]) if ctx.guild else None
            if channel:
                ignored.append(f"#{channel.name}")
    
    if ignored:
        embed.add_field(name="🚫 Ignored Channels", value=", ".join(ignored), inline=False)
    
    # Statistics
    async with bot.db.execute("SELECT COUNT(*) FROM terms WHERE guild_id=?", (gid,)) as cur:
        term_count = (await cur.fetchone())[0]
    async with bot.db.execute("SELECT COUNT(*) FROM term_categories WHERE guild_id=?", (gid,)) as cur:
        category_count = (await cur.fetchone())[0]
    async with bot.db.execute("SELECT COUNT(*) FROM term_aliases WHERE guild_id=?", (gid,)) as cur:
        alias_count = (await cur.fetchone())[0]
    
    stats = f"**Terms**: {term_count} | **Categories**: {category_count} | **Aliases**: {alias_count}"
    embed.add_field(name="📊 Current Stats", value=stats, inline=False)
    
    await ctx.send(embed=embed)

@bot.command(name="set")
@commands.check(admin_or_power)
async def cmd_set(ctx: commands.Context, setting: str, *, value: str):
    """Change a guild setting"""
    gid = ctx.guild.id if ctx.guild else 0
    setting = setting.lower()
    settings = await bot.get_guild_settings(gid)
    
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
            
    elif setting in ["theme_color", "color"]:
        try:
            # Accept hex colors
            if value.startswith('#'):
                color_val = int(value[1:], 16)
            else:
                color_val = int(value)
            
            await bot.db.execute(
                "UPDATE guild_settings SET theme_color=? WHERE guild_id=?",
                (color_val, gid)
            )
            await ctx.send(f"✅ Theme color set to #{color_val:06x}.")
        except ValueError:
            await ctx.send("❌ Please provide a valid color (hex or decimal).")
            
    elif setting in ["daily_summary"]:
        bool_val = value.lower() in ["true", "yes", "1", "on", "enable"]
        await bot.db.execute(
            "UPDATE guild_settings SET daily_summary=? WHERE guild_id=?",
            (bool_val, gid)
        )
        await ctx.send(f"✅ Daily summary {'enabled' if bool_val else 'disabled'}.")
        
    elif setting in ["notification_channel"]:
        try:
            channel_id = int(value.replace('<#', '').replace('>', ''))
            channel = ctx.guild.get_channel(channel_id)
            if not channel:
                await ctx.send("❌ Channel not found.")
                return
                
            await bot.db.execute(
                "UPDATE guild_settings SET notification_channel=? WHERE guild_id=?",
                (channel_id, gid)
            )
            await ctx.send(f"✅ Notification channel set to {channel.mention}.")
        except ValueError:
            await ctx.send("❌ Please provide a valid channel mention or ID.")
    else:
        await ctx.send("❌ Unknown setting. Available: ignore_commands, case_sensitive, min_word_length, cooldown, theme_color, daily_summary, notification_channel")
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
    
    now = datetime.now(timezone.utc).isoformat()
    await bot.db.execute(
        "INSERT INTO ignored_channels(guild_id, channel_id, ignored_by, ignored_at) VALUES(?, ?, ?, ?)",
        (gid, channel.id, ctx.author.id, now)
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

@bot.command(name="recent")
async def cmd_recent(ctx: commands.Context, term: str = None, limit: int = 5):
    """Show recent messages containing a term"""
    gid = ctx.guild.id if ctx.guild else 0
    limit = max(1, min(20, limit))
    settings = await bot.get_guild_settings(gid)
    
    if term:
        term = await bot.resolve_term(gid, normalize_term(term))
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
        color=settings['theme_color']
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

@bot.command(name="leaderboard", aliases=["lb", "top"])
async def cmd_leaderboard(ctx: commands.Context, timeframe: str = "all"):
    """Show user leaderboard for all terms"""
    gid = ctx.guild.id if ctx.guild else 0
    settings = await bot.get_guild_settings(gid)
    
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
        color=settings['theme_color']
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
    settings = await bot.get_guild_settings(gid)
    
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
        color=settings['theme_color']
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
    settings = await bot.get_guild_settings(gid)
    
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
        await bot.db.execute("DELETE FROM daily_stats WHERE guild_id=? AND term=?", (gid, term))
        await bot.db.commit()
        
        embed = discord.Embed(
            title="Statistics Reset",
            description=f"Reset statistics for `{term}` ({total_count} mentions cleared)",
            color=settings['theme_color']
        )
        await ctx.send(embed=embed)
    else:
        # Reset all terms for this guild
        async with bot.db.execute("SELECT COUNT(*) FROM messages WHERE guild_id=?", (gid,)) as cur:
            total_messages = (await cur.fetchone())[0]
        
        tables_to_reset = ["term_meta", "hits", "messages", "user_cooldowns", "daily_stats"]
        for table in tables_to_reset:
            await bot.db.execute(f"DELETE FROM {table} WHERE guild_id=?", (gid,))
        
        await bot.db.commit()
        
        embed = discord.Embed(
            title="All Statistics Reset",
            description=f"Reset all statistics for this server ({total_messages} total messages cleared)",
            color=settings['theme_color']
        )
        await ctx.send(embed=embed)

@bot.command(name="help", aliases=["h"])
async def cmd_help(ctx: commands.Context, command: str = None):
    """Show help information"""
    settings = await bot.get_guild_settings(ctx.guild.id if ctx.guild else 0)
    
    if command:
        # Show help for specific command
        cmd_obj = bot.get_command(command)
        if not cmd_obj:
            await ctx.send(f"❌ Unknown command: `{command}`")
            return
        
        embed = discord.Embed(
            title=f"Help: `{COMMAND_PREFIX}{command}`", 
            color=settings['theme_color']
        )
        embed.description = cmd_obj.help or "No description available"
        
        if hasattr(cmd_obj, 'aliases') and cmd_obj.aliases:
            embed.add_field(name="Aliases", value=", ".join(f"`{a}`" for a in cmd_obj.aliases), inline=False)
        
        await ctx.send(embed=embed)
        return
    
    # General help
    embed = discord.Embed(
        title="🤖 Term Tracker Bot Help", 
        color=settings['theme_color']
    )
    embed.description = "Track and count mentions of specific terms across your server!"
    
    # Basic commands
    basic = [
        f"`{COMMAND_PREFIX}terms` - List tracked terms",
        f"`{COMMAND_PREFIX}stats [term]` - Show statistics",
        f"`{COMMAND_PREFIX}recent [term] [limit]` - Show recent messages",
        f"`{COMMAND_PREFIX}leaderboard [timeframe]` - User rankings",
        f"`{COMMAND_PREFIX}search <query>` - Search messages",
        f"`{COMMAND_PREFIX}achievements [@user]` - View achievements",
        f"`{COMMAND_PREFIX}dashboard` - Server overview"
    ]
    embed.add_field(name="📊 Basic Commands", value="\n".join(basic), inline=False)
    
    # Admin commands
    admin = [
        f"`{COMMAND_PREFIX}track <term>` - Start tracking a term",
        f"`{COMMAND_PREFIX}untrack <term>` - Stop tracking a term",
        f"`{COMMAND_PREFIX}category` - Manage categories",
        f"`{COMMAND_PREFIX}alias <alias> <term>` - Create alias",
        f"`{COMMAND_PREFIX}settings` - Show guild settings",
        f"`{COMMAND_PREFIX}set <setting> <value>` - Change settings"
    ]
    embed.add_field(name="⚙️ Admin Commands", value="\n".join(admin), inline=False)
    
    embed.add_field(name="💡 Tips", 
                    value="• Use categories to organize terms\n• Set up aliases for common variations\n• Check the web dashboard for detailed analytics", 
                    inline=False)
    
    embed.set_footer(text=f"Use {COMMAND_PREFIX}help <command> for detailed help")
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