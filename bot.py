import os
import re
import json
import logging
from datetime import datetime, timezone
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
        super().__init__(command_prefix=COMMAND_PREFIX, intents=intents)
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

    async def refresh_patterns(self):
        self.patterns.clear()
        # Load all guild/term pairs
        async with self.db.execute("SELECT guild_id, term FROM terms ORDER BY guild_id, term") as cur:
            rows = await cur.fetchall()
        from collections import defaultdict
        tmp = defaultdict(list)
        for gid, term in rows:
            tmp[gid].append(term)
        for gid, terms in tmp.items():
            self.patterns[gid] = build_patterns(terms)

    async def on_ready(self):
        log.info("Logged in as %s (%s)", self.user, self.user.id)
        async with self.db.execute("SELECT COUNT(*), COUNT(DISTINCT guild_id) FROM terms") as cur:
            total_terms, guilds = await cur.fetchone()
        log.info("Tracking %s term(s) across %s guild(s).", total_terms, guilds)

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
        pats = self.patterns.get(gid, [])
        matched_any = False
        for term, pat in pats:
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
    gid = ctx.guild.id if ctx.guild else 0
    terms: List[str] = []
    async with bot.db.execute("SELECT term FROM terms WHERE guild_id=? ORDER BY term", (gid,)) as cur:
        async for row in cur:
            terms.append(row[0])
    if not terms:
        await ctx.send("No tracked terms yet. Use `!track <term>` to add one.")
    else:
        await ctx.send("**Tracked terms:** " + ", ".join(f"`{t}`" for t in terms))

@bot.command(name="track")
@commands.check(admin_or_power)
async def cmd_track(ctx: commands.Context, *, term: str):
    gid = ctx.guild.id if ctx.guild else 0
    term = normalize_term(term)
    await bot.db.execute("INSERT OR IGNORE INTO terms(guild_id, term) VALUES(?, ?)", (gid, term))
    await bot.db.commit()
    await bot.refresh_patterns()
    await ctx.send(f"✅ Now tracking `{term}`.")

@bot.command(name="untrack")
@commands.check(admin_or_power)
async def cmd_untrack(ctx: commands.Context, *, term: str):
    gid = ctx.guild.id if ctx.guild else 0
    term = normalize_term(term)
    await bot.db.execute("DELETE FROM terms WHERE guild_id=? AND term=?", (gid, term))
    await bot.db.execute("DELETE FROM term_meta WHERE guild_id=? AND term=?", (gid, term))
    await bot.db.execute("DELETE FROM hits WHERE guild_id=? AND term=?", (gid, term))
    await bot.db.commit()
    await bot.refresh_patterns()
    await ctx.send(f"🗑️ Removed `{term}` from tracked terms and deleted its stats.")

@bot.command(name="stats")
async def cmd_stats(ctx: commands.Context, *, term: str = None):
    gid = ctx.guild.id if ctx.guild else 0
    if term:
        term = normalize_term(term)
        rows = []
        async with bot.db.execute(
            "SELECT user_name, count, last_seen FROM hits WHERE guild_id=? AND term=? ORDER BY count DESC LIMIT 10", (gid, term)
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
            "SELECT term, total_count FROM term_meta WHERE guild_id=? ORDER BY total_count DESC LIMIT 10", (gid,)
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
    gid = ctx.guild.id if ctx.guild else 0
    phrase = normalize_term(phrase)
    await bot.db.execute("INSERT OR IGNORE INTO forbidden_phrases(guild_id, phrase) VALUES(?, ?)", (gid, phrase))
    await bot.db.commit()
    await ctx.send(f"✅ Added forbidden phrase `{phrase}` for this server.")

@bot.command(name="forbidden_remove")
@commands.check(admin_or_power)
async def cmd_forbidden_remove(ctx: commands.Context, *, phrase: str):
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
    gid = ctx.guild.id if ctx.guild else 0
    phrase = normalize_term(phrase)
    await bot.db.execute("INSERT OR IGNORE INTO timeout_phrases(guild_id, phrase) VALUES(?, ?)", (gid, phrase))
    await bot.db.commit()
    await ctx.send(f"✅ Added timeout phrase `{phrase}` for this server.")

@bot.command(name="timeouts_remove")
@commands.check(admin_or_power)
async def cmd_timeouts_remove(ctx: commands.Context, *, phrase: str):
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
    forbidden = [r[0] async for r in await bot.db.execute("SELECT phrase FROM forbidden_phrases WHERE guild_id=? ORDER BY phrase", (gid,))]
    timeouts  = [r[0] async for r in await bot.db.execute("SELECT phrase FROM timeout_phrases WHERE guild_id=? ORDER BY phrase", (gid,))]
    responses = []
    async with bot.db.execute("SELECT keyword, response FROM keyword_responses WHERE guild_id=? ORDER BY keyword", (gid,)) as cur:
        async for k, v in cur:
            responses.append({"keyword": k, "response": v})

    payload = {
        "guild_id": gid,
        "tracked_terms": terms,
        "term_data": term_data,
        "forbidden_phrases": forbidden,
        "timeout_phrases": timeouts,
        "keyword_responses": responses,
    }

    with tempfile.NamedTemporaryFile("w", delete=False, suffix=f"-guild{gid}.json") as fp:
        json.dump(payload, fp, indent=2)
        tmp_path = fp.name
    await ctx.send(file=discord.File(tmp_path, filename=os.path.basename(tmp_path)))
    os.unlink(tmp_path)

@bot.command(name="import")
@commands.check(admin_or_power)
async def cmd_import(ctx: commands.Context):
    import json
    gid = ctx.guild.id if ctx.guild else 0
    if not ctx.message.attachments:
        await ctx.send("Attach a JSON export file to import.")
        return
    data_bytes = await ctx.message.attachments[0].read()
    try:
        data = json.loads(data_bytes.decode("utf-8"))
    except Exception as e:
        await ctx.send(f"Couldn't parse JSON: {e}")
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

    await bot.db.commit()
    await bot.refresh_patterns()
    await ctx.send("✅ Import complete for this server.")

def check_power(ctx: commands.Context) -> bool:
    return is_power_user(ctx.author)

@bot.command(name="pu_guilds")
@commands.check(check_power)
async def cmd_pu_guilds(ctx: commands.Context):
    lines = [f"{g.name} — {g.id}" for g in bot.guilds]
    if not lines:
        await ctx.send("Bot is not in any guilds.")
    else:
        await ctx.send("**Guilds:**\n" + "\n".join(lines[:50]))

@bot.command(name="pu_all_terms")
@commands.check(check_power)
async def cmd_pu_all_terms(ctx: commands.Context, limit: int = 20):
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
    lines = [f"{i+1}. `{t}` — {c}" for i, (t, c) in enumerate(rows)]
    await ctx.send("**Global top terms:**\n" + "\n".join(lines))

@bot.command(name="pu_all_stats")
@commands.check(check_power)
async def cmd_pu_all_stats(ctx: commands.Context, term: str, limit: int = 20):
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
    lines = [f"{i+1}. **{user}** — {c}" for i, (user, c) in enumerate(rows)]
    await ctx.send(f"**Global users for `{term}`:**\n" + "\n".join(lines))

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