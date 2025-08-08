import os
import re
import json
import logging
from datetime import datetime, timezone
from typing import List, Tuple

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

# Case-insensitive matching enforced:
CASE_SENSITIVE = False

# Path for migration (your existing JSON file)
JSON_PATH = os.getenv("JSON_PATH", "bot_data.json")

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
    term TEXT PRIMARY KEY
);
CREATE TABLE IF NOT EXISTS term_meta (
    term TEXT PRIMARY KEY,
    total_count INTEGER NOT NULL DEFAULT 0,
    last_mentioned TEXT,
    last_user TEXT
);
CREATE TABLE IF NOT EXISTS hits (
    term TEXT NOT NULL,
    user TEXT NOT NULL,
    count INTEGER NOT NULL DEFAULT 0,
    last_seen TEXT,
    PRIMARY KEY (term, user)
);
CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    guild_id INTEGER NOT NULL,
    channel_id INTEGER NOT NULL,
    user TEXT NOT NULL,
    message_id INTEGER NOT NULL,
    term TEXT NOT NULL,
    content TEXT NOT NULL,
    created_at TEXT NOT NULL
);
-- NEW: moderation/response config persisted in SQLite
CREATE TABLE IF NOT EXISTS forbidden_phrases (
    phrase TEXT PRIMARY KEY
);
CREATE TABLE IF NOT EXISTS timeout_phrases (
    phrase TEXT PRIMARY KEY
);
CREATE TABLE IF NOT EXISTS keyword_responses (
    keyword TEXT PRIMARY KEY,
    response TEXT NOT NULL
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

def build_patterns(terms: List[str]) -> List[Tuple[str, re.Pattern]]:
    flags = 0 if CASE_SENSITIVE else re.IGNORECASE
    return [(t, re.compile(re.escape(t), flags)) for t in terms]

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

    # Insert terms
    for term in tracked_terms or term_data.keys():
        norm = normalize_term(term)
        await db.execute("INSERT OR IGNORE INTO terms(term) VALUES(?)", (norm,))

    # Insert aggregates + per-user counts
    for term, info in term_data.items():
        norm = normalize_term(term)
        total = int(info.get("count", 0) or 0)
        last_mentioned = info.get("last_mentioned")
        last_user = info.get("last_user")
        await db.execute(
            "INSERT OR REPLACE INTO term_meta(term, total_count, last_mentioned, last_user) "
            "VALUES(?,?,?,?)",
            (norm, total, last_mentioned, last_user)
        )
        for user, cnt in (info.get("user_counts") or {}).items():
            await db.execute(
                "INSERT OR REPLACE INTO hits(term, user, count, last_seen) VALUES(?,?,?,?)",
                (norm, user, int(cnt or 0), last_mentioned)
            )

    # Persist forbidden phrases
    for phrase in (data.get("forbidden_phrases") or []):
        await db.execute(
            "INSERT OR IGNORE INTO forbidden_phrases(phrase) VALUES(?)",
            (normalize_term(phrase),)
        )

    # Persist timeout phrases
    for phrase in (data.get("timeout_phrases") or []):
        await db.execute(
            "INSERT OR IGNORE INTO timeout_phrases(phrase) VALUES(?)",
            (normalize_term(phrase),)
        )

    # Persist keyword responses
    for k, v in (data.get("keyword_responses") or {}).items():
        await db.execute(
            "INSERT OR REPLACE INTO keyword_responses(keyword, response) VALUES(?,?)",
            (normalize_term(k), str(v))
        )

    await db.commit()
    log.info("Migration from JSON complete. You can keep bot_data.json as a backup or delete it later.")

# -------------------------
# Bot
# -------------------------
class TermBot(commands.Bot):
    def __init__(self):
        super().__init__(command_prefix=COMMAND_PREFIX, intents=intents)
        self.db: aiosqlite.Connection | None = None
        self.patterns: List[Tuple[str, re.Pattern]] = []

    async def setup_hook(self) -> None:
        self.db = await aiosqlite.connect(DB_PATH)
        await init_db(self.db)
        if await needs_migration(self.db):
            await migrate_json(self.db)
        await self.refresh_patterns()

    async def refresh_patterns(self):
        terms: List[str] = []
        async with self.db.execute("SELECT term FROM terms ORDER BY term") as cur:
            async for row in cur:
                terms.append(row[0])
        self.patterns = build_patterns(terms)

    async def on_ready(self):
        log.info("Logged in as %s (%s)", self.user, self.user.id)
        async with self.db.execute("SELECT COUNT(*) FROM terms") as cur:
            n = (await cur.fetchone())[0]
        log.info("Tracking %d term(s).", n)

    async def increment(self, message: discord.Message, term: str, occurrences: int = 1):
        term = normalize_term(term)
        now = datetime.now(timezone.utc).isoformat()
        user = str(message.author)

        # Totals
        await self.db.execute(
            "INSERT INTO term_meta(term, total_count, last_mentioned, last_user) "
            "VALUES(?,?,?,?) "
            "ON CONFLICT(term) DO UPDATE SET "
            "total_count = term_meta.total_count + excluded.total_count, "
            "last_mentioned = excluded.last_mentioned, "
            "last_user = excluded.last_user",
            (term, occurrences, now, user)
        )

        # Per-user
        await self.db.execute(
            "INSERT INTO hits(term, user, count, last_seen) "
            "VALUES(?,?,?,?) "
            "ON CONFLICT(term, user) DO UPDATE SET "
            "count = hits.count + excluded.count, "
            "last_seen = excluded.last_seen",
            (term, user, occurrences, now)
        )

        # Message log
        await self.db.execute(
            "INSERT INTO messages(guild_id, channel_id, user, message_id, term, content, created_at) "
            "VALUES(?,?,?,?,?,?,?)",
            (message.guild.id if message.guild else 0, message.channel.id, user, message.id,
             term, message.content, now)
        )

    async def on_message(self, message: discord.Message):
        if message.author.bot:
            return

        content = message.content or ""
        if not content:
            return

        matched_any = False
        for term, pat in self.patterns:
            if pat.search(content):  # case-insensitive via pattern flags
                await self.increment(message, term, occurrences=1)
                matched_any = True

        if matched_any:
            await self.db.commit()

        await self.process_commands(message)

bot = TermBot()

# -------------------------
# Commands
# -------------------------
@bot.command(name="terms")
async def cmd_terms(ctx: commands.Context):
    terms: List[str] = []
    async with bot.db.execute("SELECT term FROM terms ORDER BY term") as cur:
        async for row in cur:
            terms.append(row[0])
    if not terms:
        await ctx.send("No tracked terms yet. Use `!track <term>` to add one.")
    else:
        await ctx.send("**Tracked terms:** " + ", ".join(f"`{t}`" for t in terms))

@bot.command(name="track")
@commands.has_permissions(administrator=True)
async def cmd_track(ctx: commands.Context, *, term: str):
    term = normalize_term(term)
    await bot.db.execute("INSERT OR IGNORE INTO terms(term) VALUES(?)", (term,))
    await bot.db.commit()
    await bot.refresh_patterns()
    await ctx.send(f"✅ Now tracking `{term}`.")

@bot.command(name="untrack")
@commands.has_permissions(administrator=True)
async def cmd_untrack(ctx: commands.Context, *, term: str):
    term = normalize_term(term)
    await bot.db.execute("DELETE FROM terms WHERE term = ?", (term,))
    await bot.db.execute("DELETE FROM term_meta WHERE term = ?", (term,))
    await bot.db.execute("DELETE FROM hits WHERE term = ?", (term,))
    await bot.db.commit()
    await bot.refresh_patterns()
    await ctx.send(f"🗑️ Removed `{term}` from tracked terms and deleted its stats.")

@bot.command(name="stats")
async def cmd_stats(ctx: commands.Context, *, term: str = None):
    if term:
        term = normalize_term(term)
        rows = []
        async with bot.db.execute(
            "SELECT user, count, last_seen FROM hits WHERE term = ? ORDER BY count DESC LIMIT 10", (term,)
        ) as cur:
            async for r in cur:
                rows.append(r)
        if not rows:
            await ctx.send(f"No data yet for `{term}`.")
            return
        lines = [f"{i+1}. **{user}** — {count} (last: {last_seen})" for i, (user, count, last_seen) in enumerate(rows)]
        await ctx.send(f"Top users for `{term}`:\n" + "\n".join(lines))
    else:
        rows = []
        async with bot.db.execute(
            "SELECT term, total_count FROM term_meta ORDER BY total_count DESC LIMIT 10"
        ) as cur:
            async for r in cur:
                rows.append(r)
        if not rows:
            await ctx.send("No data yet.")
            return
        lines = [f"{i+1}. `{t}` — {c}" for i, (t, c) in enumerate(rows)]
        await ctx.send("Top terms:\n" + "\n".join(lines))

# -------------------------
# Additional Moderation/Response Commands
# -------------------------

@bot.command(name="forbidden")
async def cmd_forbidden(ctx: commands.Context):
    phrases = []
    async with bot.db.execute("SELECT phrase FROM forbidden_phrases ORDER BY phrase") as cur:
        async for r in cur:
            phrases.append(r[0])
    if not phrases:
        await ctx.send("No forbidden phrases configured.")
    else:
        await ctx.send("**Forbidden phrases:** " + ", ".join(f"`{p}`" for p in phrases))

@bot.command(name="timeouts")
async def cmd_timeouts(ctx: commands.Context):
    phrases = []
    async with bot.db.execute("SELECT phrase FROM timeout_phrases ORDER BY phrase") as cur:
        async for r in cur:
            phrases.append(r[0])
    if not phrases:
        await ctx.send("No timeout phrases configured.")
    else:
        await ctx.send("**Timeout phrases:** " + ", ".join(f"`{p}`" for p in phrases))

@bot.command(name="responses")
async def cmd_responses(ctx: commands.Context):
    pairs = []
    async with bot.db.execute("SELECT keyword, response FROM keyword_responses ORDER BY keyword") as cur:
        async for r in cur:
            pairs.append((r[0], r[1]))
    if not pairs:
        await ctx.send("No keyword responses configured.")
    else:
        lines = [f"`{k}` → {v}" for k, v in pairs[:25]]
        await ctx.send("**Keyword responses:**\n" + "\n".join(lines))

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