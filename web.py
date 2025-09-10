import os, sqlite3
from flask import Flask, jsonify, request, g, abort, render_template_string, redirect, url_for
from jinja2 import ChoiceLoader, DictLoader
from datetime import datetime, timedelta
import json

DB_PATH = os.getenv("DB_PATH", "termbot.sqlite3")

app = Flask(__name__)

# ----------------------
# DB helpers
# ----------------------

def get_db():
    db = getattr(g, "_db", None)
    if db is None:
        db = g._db = sqlite3.connect(DB_PATH, check_same_thread=False)
        db.row_factory = sqlite3.Row
    return db

@app.teardown_appcontext
def close_db(exc):
    db = getattr(g, "_db", None)
    if db is not None:
        db.close()

# ----------------------
# Enhanced JSON API
# ----------------------
@app.get("/healthz")
def health():
    return {"ok": True, "timestamp": datetime.now().isoformat()}

@app.get("/api/guilds")
def guilds_json():
    db = get_db()
    rows = db.execute(
        """
      SELECT guild_id,
             COUNT(DISTINCT term) AS terms,
             COALESCE(SUM(total_count),0) AS mentions,
             COUNT(DISTINCT tc.category_name) AS categories,
             COUNT(DISTINCT ta.alias) AS aliases
      FROM term_meta tm
      LEFT JOIN term_categories tc ON tm.guild_id = tc.guild_id
      LEFT JOIN term_aliases ta ON tm.guild_id = ta.guild_id
      GROUP BY guild_id
      ORDER BY mentions DESC
    """
    ).fetchall()
    return jsonify([dict(r) for r in rows])

@app.get("/api/guild/<int:gid>/stats")
def guild_stats_json(gid):
    db = get_db()
    
    # Get comprehensive guild statistics
    stats = {}
    
    # Basic counts
    basic_stats = db.execute(
        """SELECT COUNT(DISTINCT t.term) as terms, 
                  COALESCE(SUM(tm.total_count), 0) as total_mentions,
                  COUNT(DISTINCT h.user_id) as active_users,
                  COUNT(DISTINCT tc.category_name) as categories
           FROM terms t
           LEFT JOIN term_meta tm ON t.guild_id = tm.guild_id AND t.term = tm.term
           LEFT JOIN hits h ON t.guild_id = h.guild_id AND t.term = h.term
           LEFT JOIN term_categories tc ON t.guild_id = tc.guild_id
           WHERE t.guild_id = ?""",
        (gid,)
    ).fetchone()
    
    stats.update(dict(basic_stats))
    
    # Recent activity (last 7 days)
    week_ago = (datetime.now() - timedelta(days=7)).isoformat()
    recent_activity = db.execute(
        """SELECT COUNT(*) as recent_mentions,
                  COUNT(DISTINCT user_id) as recent_users
           FROM messages 
           WHERE guild_id = ? AND created_at >= ?""",
        (gid, week_ago)
    ).fetchone()
    
    stats.update(dict(recent_activity))
    
    return jsonify(stats)

@app.get("/api/guild/<int:gid>/top_terms")
def top_terms_json(gid):
    limit = min(int(request.args.get("limit", 20)), 100)
    timeframe = request.args.get("timeframe", "all")  # all, week, month
    
    db = get_db()
    
    if timeframe == "week":
        cutoff = (datetime.now() - timedelta(days=7)).isoformat()
        rows = db.execute(
            """SELECT term, COUNT(*) as count
               FROM messages
               WHERE guild_id = ? AND created_at >= ?
               GROUP BY term
               ORDER BY count DESC
               LIMIT ?""",
            (gid, cutoff, limit)
        ).fetchall()
    elif timeframe == "month":
        cutoff = (datetime.now() - timedelta(days=30)).isoformat()
        rows = db.execute(
            """SELECT term, COUNT(*) as count
               FROM messages
               WHERE guild_id = ? AND created_at >= ?
               GROUP BY term
               ORDER BY count DESC
               LIMIT ?""",
            (gid, cutoff, limit)
        ).fetchall()
    else:
        rows = db.execute(
            """SELECT term, total_count as count
               FROM term_meta
               WHERE guild_id = ?
               ORDER BY total_count DESC
               LIMIT ?""",
            (gid, limit)
        ).fetchall()
    
    return jsonify([dict(r) for r in rows])

@app.get("/api/guild/<int:gid>/term/<term>/leaderboard")
def term_leaderboard_json(gid, term):
    limit = min(int(request.args.get("limit", 20)), 100)
    term = term.lower()
    db = get_db()
    rows = db.execute(
        """
      SELECT user_id, user_name, count, last_seen
      FROM hits
      WHERE guild_id = ? AND term = ?
      ORDER BY count DESC
      LIMIT ?
    """,
        (gid, term, limit),
    ).fetchall()
    return jsonify([dict(r) for r in rows])

@app.get("/api/guild/<int:gid>/trends")
def trends_json(gid):
    days = min(int(request.args.get("days", 7)), 90)
    cutoff = (datetime.now() - timedelta(days=days)).isoformat()
    
    db = get_db()
    rows = db.execute(
        """SELECT DATE(created_at) as date, term, COUNT(*) as mentions
           FROM messages
           WHERE guild_id = ? AND created_at >= ?
           GROUP BY DATE(created_at), term
           ORDER BY date, mentions DESC""",
        (gid, cutoff)
    ).fetchall()
    
    return jsonify([dict(r) for r in rows])

@app.get("/api/search")
def search_json():
    q = request.args.get("q", "").strip()
    if not q:
        abort(400, "q required")
    gid = request.args.get("gid")
    limit = min(int(request.args.get("limit", 100)), 1000)
    like = f"%{q}%"
    db = get_db()
    if gid is None:
        rows = db.execute(
            """
          SELECT guild_id, channel_id, user_name, term,
                 substr(content,1,200) AS snippet, created_at
          FROM messages
          WHERE content LIKE ?
          ORDER BY id DESC
          LIMIT ?
        """,
            (like, limit),
        ).fetchall()
    else:
        rows = db.execute(
            """
          SELECT guild_id, channel_id, user_name, term,
                 substr(content,1,200) AS snippet, created_at
          FROM messages
          WHERE guild_id = ? AND content LIKE ?
          ORDER BY id DESC
          LIMIT ?
        """,
            (int(gid), like, limit),
        ).fetchall()
    return jsonify([dict(r) for r in rows])

@app.get("/api/guild/<int:gid>/achievements")
def achievements_json(gid):
    db = get_db()
    
    # Get top achievers
    top_achievers = db.execute(
        """SELECT h.user_name, COUNT(DISTINCT ua.achievement_id) as achievement_count,
                  SUM(h.count) as total_mentions
           FROM hits h
           LEFT JOIN user_achievements ua ON h.guild_id = ua.guild_id AND h.user_id = ua.user_id
           WHERE h.guild_id = ?
           GROUP BY h.user_id, h.user_name
           ORDER BY achievement_count DESC, total_mentions DESC
           LIMIT 10""",
        (gid,)
    ).fetchall()
    
    # Get recent achievements
    recent_achievements = db.execute(
        """SELECT ua.user_id, h.user_name, a.name, a.badge_emoji, ua.earned_at
           FROM user_achievements ua
           JOIN achievements a ON ua.achievement_id = a.id
           LEFT JOIN hits h ON ua.guild_id = h.guild_id AND ua.user_id = h.user_id
           WHERE ua.guild_id = ?
           ORDER BY ua.earned_at DESC
           LIMIT 20""",
        (gid,)
    ).fetchall()
    
    return jsonify({
        "top_achievers": [dict(r) for r in top_achievers],
        "recent_achievements": [dict(r) for r in recent_achievements]
    })

# ----------------------
# Enhanced UI with Modern Design
# ----------------------
BASE = """<!doctype html>
<html lang="en" data-bs-theme="auto">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{{ title or 'TermBot Dashboard' }}</title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet" />
  <link href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/bootstrap-icons.css" rel="stylesheet" />
  <style>
    :root {
      --bs-primary-rgb: 88, 101, 242;
      --bs-success-rgb: 34, 197, 94;
      --bs-info-rgb: 59, 130, 246;
      --gradient-primary: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
      --gradient-success: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    }
    
    body {
      background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%);
      min-height: 100vh;
    }
    
    .navbar {
      background: var(--gradient-primary) !important;
      backdrop-filter: blur(10px);
      border-bottom: 1px solid rgba(255,255,255,0.1);
    }
    
    .navbar-brand {
      font-weight: 700;
      font-size: 1.5rem;
      text-shadow: 0 2px 4px rgba(0,0,0,0.3);
    }
    
    .card {
      border: none;
      box-shadow: 0 10px 30px rgba(0,0,0,0.1);
      backdrop-filter: blur(10px);
      background: rgba(255,255,255,0.9);
      border-radius: 16px;
      transition: transform 0.3s ease, box-shadow 0.3s ease;
    }
    
    .card:hover {
      transform: translateY(-5px);
      box-shadow: 0 20px 40px rgba(0,0,0,0.15);
    }
    
    .card-header {
      background: var(--gradient-primary);
      color: white;
      border: none;
      border-radius: 16px 16px 0 0 !important;
      font-weight: 600;
    }
    
    .stat-card {
      background: var(--gradient-primary);
      color: white;
      text-align: center;
      border-radius: 16px;
      padding: 2rem 1rem;
    }
    
    .stat-number {
      font-size: 2.5rem;
      font-weight: 700;
      text-shadow: 0 2px 4px rgba(0,0,0,0.3);
    }
    
    .stat-label {
      opacity: 0.9;
      font-weight: 500;
      text-transform: uppercase;
      font-size: 0.85rem;
      letter-spacing: 1px;
    }
    
    .progress {
      height: 8px;
      border-radius: 10px;
      background: rgba(255,255,255,0.2);
    }
    
    .progress-bar {
      border-radius: 10px;
      background: linear-gradient(90deg, #667eea, #764ba2);
    }
    
    .badge-custom {
      background: var(--gradient-primary);
      color: white;
      font-weight: 500;
      border-radius: 8px;
      padding: 0.5rem 0.75rem;
    }
    
    .btn-primary {
      background: var(--gradient-primary);
      border: none;
      border-radius: 10px;
      font-weight: 500;
      padding: 0.5rem 1.5rem;
      transition: all 0.3s ease;
    }
    
    .btn-primary:hover {
      transform: translateY(-2px);
      box-shadow: 0 5px 15px rgba(88, 101, 242, 0.4);
    }
    
    .table {
      border-radius: 12px;
      overflow: hidden;
      box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    
    .table thead th {
      background: var(--gradient-primary);
      color: white;
      border: none;
      font-weight: 600;
      text-transform: uppercase;
      font-size: 0.85rem;
      letter-spacing: 0.5px;
    }
    
    .table tbody tr {
      transition: background-color 0.2s ease;
    }
    
    .table tbody tr:hover {
      background-color: rgba(88, 101, 242, 0.1);
    }
    
    .search-container {
      background: rgba(255,255,255,0.9);
      backdrop-filter: blur(10px);
      border-radius: 16px;
      padding: 2rem;
      box-shadow: 0 10px 30px rgba(0,0,0,0.1);
    }
    
    .form-control, .form-select {
      border: 2px solid rgba(88, 101, 242, 0.2);
      border-radius: 10px;
      padding: 0.75rem 1rem;
      transition: all 0.3s ease;
    }
    
    .form-control:focus, .form-select:focus {
      border-color: rgba(88, 101, 242, 0.5);
      box-shadow: 0 0 0 0.25rem rgba(88, 101, 242, 0.1);
    }
    
    .footer {
      background: rgba(255,255,255,0.9);
      backdrop-filter: blur(10px);
      border-top: 1px solid rgba(0,0,0,0.1);
      margin-top: 3rem;
      padding: 2rem 0;
      color: #6c757d;
    }
    
    .achievement-badge {
      display: inline-block;
      background: linear-gradient(135deg, #ffd700, #ffa500);
      color: #333;
      padding: 0.25rem 0.75rem;
      border-radius: 20px;
      font-size: 0.85rem;
      font-weight: 600;
      margin: 0.25rem;
      box-shadow: 0 2px 8px rgba(255, 215, 0, 0.3);
    }
    
    .trend-indicator {
      display: inline-block;
      width: 100px;
      height: 20px;
      background: rgba(88, 101, 242, 0.2);
      border-radius: 10px;
      overflow: hidden;
      margin: 0 0.5rem;
    }
    
    .trend-bar {
      height: 100%;
      background: var(--gradient-primary);
      border-radius: 10px;
      transition: width 0.5s ease;
    }
    
    .loading {
      text-align: center;
      padding: 3rem;
      color: #6c757d;
    }
    
    .spinner-border {
      color: #5865f2;
    }
    
    @media (max-width: 768px) {
      .stat-card {
        margin-bottom: 1rem;
      }
      
      .card {
        margin-bottom: 1rem;
      }
    }
  </style>
</head>
<body>
<nav class="navbar navbar-expand-lg navbar-dark fixed-top">
  <div class="container">
    <a class="navbar-brand d-flex align-items-center" href="{{ url_for('home') }}">
      <i class="bi bi-robot me-2"></i>
      TermBot Dashboard
    </a>
    <button class="navbar-toggler" type="button" data-bs-toggle="collapse" data-bs-target="#navbarNav">
      <span class="navbar-toggler-icon"></span>
    </button>
    <div class="collapse navbar-collapse" id="navbarNav">
      <ul class="navbar-nav ms-auto">
        <li class="nav-item">
          <a class="nav-link" href="{{ url_for('ui_guilds') }}">
            <i class="bi bi-servers me-1"></i>Servers
          </a>
        </li>
        <li class="nav-item">
          <a class="nav-link" href="{{ url_for('ui_search') }}">
            <i class="bi bi-search me-1"></i>Search
          </a>
        </li>
        <li class="nav-item">
          <a class="nav-link" href="{{ url_for('ui_analytics') }}">
            <i class="bi bi-graph-up me-1"></i>Analytics
          </a>
        </li>
      </ul>
    </div>
  </div>
</nav>

<main class="container-fluid" style="padding-top: 100px;">
  {% block content %}{% endblock %}
</main>

<footer class="footer">
  <div class="container">
    <div class="row align-items-center">
      <div class="col-md-8">
        <p class="mb-0">
          <i class="bi bi-database me-1"></i>
          Database: {{ db_path }} • 
          <a href="/api/guilds" class="text-decoration-none">JSON API</a> available
        </p>
      </div>
      <div class="col-md-4 text-end">
        <small>Powered by TermBot v2.0</small>
      </div>
    </div>
  </div>
</footer>

<script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/js/bootstrap.bundle.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/dayjs@1/dayjs.min.js"></script>
<script>
// Theme toggle
const setTheme = theme => {
  document.documentElement.setAttribute('data-bs-theme', theme);
  localStorage.setItem('theme', theme);
};

const getPreferredTheme = () => {
  const storedTheme = localStorage.getItem('theme');
  if (storedTheme) return storedTheme;
  return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
};

setTheme(getPreferredTheme());

// Utility functions for formatting
function formatNumber(num) {
  if (num >= 1000000) return (num / 1000000).toFixed(1) + 'M';
  if (num >= 1000) return (num / 1000).toFixed(1) + 'K';
  return num.toString();
}

function formatDate(dateString) {
  return dayjs(dateString).format('MMM D, YYYY');
}

function timeAgo(dateString) {
  return dayjs(dateString).fromNow();
}
</script>
</body>
</html>"""

# Register the base template
app.jinja_loader = ChoiceLoader([app.jinja_loader, DictLoader({'_base.html': BASE})])

@app.get("/")
def home():
    return redirect(url_for("ui_guilds"))

@app.get("/ui/guilds")
def ui_guilds():
    db = get_db()
    rows = get_db().execute("""
      SELECT
        tm.guild_id AS guild_id,
        (SELECT COUNT(DISTINCT term)
          FROM term_meta tm2
          WHERE tm2.guild_id = tm.guild_id) AS terms,
        (SELECT COALESCE(SUM(total_count),0)
          FROM term_meta tm2
          WHERE tm2.guild_id = tm.guild_id) AS mentions,
        (SELECT COUNT(DISTINCT category_name)
          FROM term_categories tc2
          WHERE tc2.guild_id = tm.guild_id) AS categories,
        (SELECT COUNT(DISTINCT alias)
          FROM term_aliases ta2
          WHERE ta2.guild_id = tm.guild_id) AS aliases
      FROM term_meta tm
      GROUP BY tm.guild_id
      ORDER BY mentions DESC
    """).fetchall()
    
    # Get total stats
    total_guilds = len(rows)
    total_terms = sum(r['terms'] for r in rows)
    total_mentions = sum(r['mentions'] for r in rows)
    
    tpl = """
    {% extends '_base.html' %}
    {% block content %}
    <div class="row mb-4">
      <div class="col-md-4">
        <div class="stat-card">
          <div class="stat-number">{{ total_guilds }}</div>
          <div class="stat-label">Active Servers</div>
        </div>
      </div>
      <div class="col-md-4">
        <div class="stat-card">
          <div class="stat-number">{{ "{:,}".format(total_terms) }}</div>
          <div class="stat-label">Tracked Terms</div>
        </div>
      </div>
      <div class="col-md-4">
        <div class="stat-card">
          <div class="stat-number">{{ "{:,}".format(total_mentions) }}</div>
          <div class="stat-label">Total Mentions</div>
        </div>
      </div>
    </div>
    
    <div class="card">
      <div class="card-header d-flex justify-content-between align-items-center">
        <h5 class="mb-0"><i class="bi bi-servers me-2"></i>Discord Servers</h5>
        <small>{{ total_guilds }} server{{ 's' if total_guilds != 1 else '' }}</small>
      </div>
      <div class="card-body p-0">
        {% if rows %}
        <div class="table-responsive">
          <table class="table table-hover mb-0">
            <thead>
              <tr>
                <th><i class="bi bi-hash"></i> Server ID</th>
                <th class="text-center"><i class="bi bi-tags"></i> Terms</th>
                <th class="text-center"><i class="bi bi-chat-dots"></i> Mentions</th>
                <th class="text-center"><i class="bi bi-folder"></i> Categories</th>
                <th class="text-center"><i class="bi bi-people"></i> Users</th>
                <th class="text-center"><i class="bi bi-clock"></i> Last Activity</th>
                <th></th>
              </tr>
            </thead>
            <tbody>
            {% for r in rows %}
              <tr>
                <td>
                  <span class="badge-custom">{{ r['guild_id'] }}</span>
                </td>
                <td class="text-center">
                  <span class="badge bg-primary">{{ r['terms'] or 0 }}</span>
                </td>
                <td class="text-center">
                  <strong>{{ "{:,}".format(r['mentions'] or 0) }}</strong>
                </td>
                <td class="text-center">
                  {{ r['categories'] or 0 }}
                </td>
                <td class="text-center">
                  {{ r['active_users'] or 0 }}
                </td>
                <td class="text-center">
                  {% if r['last_activity'] %}
                    <small class="text-muted">{{ r['last_activity'][:10] }}</small>
                  {% else %}
                    <small class="text-muted">No activity</small>
                  {% endif %}
                </td>
                <td class="text-end">
                  <a class="btn btn-primary btn-sm" href="{{ url_for('ui_guild', gid=r['guild_id']) }}">
                    <i class="bi bi-arrow-right"></i>
                  </a>
                </td>
              </tr>
            {% endfor %}
            </tbody>
          </table>
        </div>
        {% else %}
        <div class="text-center py-5">
          <i class="bi bi-server display-1 text-muted"></i>
          <h5 class="mt-3 text-muted">No servers found</h5>
          <p class="text-muted">Servers will appear here once the bot starts tracking terms.</p>
        </div>
        {% endif %}
      </div>
    </div>
    {% endblock %}
    """
    return render_template_string(
        tpl, 
        rows=rows, 
        total_guilds=total_guilds,
        total_terms=total_terms,
        total_mentions=total_mentions,
        title="Dashboard", 
        db_path=DB_PATH
    )

@app.get("/ui/guild/<int:gid>")
def ui_guild(gid: int):
    db = get_db()
    
    # Get guild overview stats
    overview = db.execute(
        """SELECT COUNT(DISTINCT t.term) as terms,
                  COALESCE(SUM(tm.total_count), 0) as total_mentions,
                  COUNT(DISTINCT h.user_id) as active_users,
                  COUNT(DISTINCT tc.category_name) as categories
           FROM terms t
           LEFT JOIN term_meta tm ON t.guild_id = tm.guild_id AND t.term = tm.term
           LEFT JOIN hits h ON t.guild_id = h.guild_id
           LEFT JOIN term_categories tc ON t.guild_id = tc.guild_id
           WHERE t.guild_id = ?""",
        (gid,)
    ).fetchone()
    
    # Get recent activity (last 7 days)
    week_ago = (datetime.now() - timedelta(days=7)).isoformat()
    recent_stats = db.execute(
        """SELECT COUNT(*) as recent_mentions,
                  COUNT(DISTINCT user_id) as recent_users
           FROM messages
           WHERE guild_id = ? AND created_at >= ?""",
        (gid, week_ago)
    ).fetchone()
    
    # Get top terms
    limit = min(int(request.args.get("limit", 15)), 50)
    top = db.execute(
        """SELECT tm.term, tm.total_count, tca.category_name,
                  COUNT(DISTINCT h.user_id) as unique_users
           FROM term_meta tm
           LEFT JOIN term_category_assignments tca ON tm.guild_id = tca.guild_id AND tm.term = tca.term
           LEFT JOIN hits h ON tm.guild_id = h.guild_id AND tm.term = h.term
           WHERE tm.guild_id = ?
           GROUP BY tm.term, tm.total_count, tca.category_name
           ORDER BY tm.total_count DESC
           LIMIT ?""",
        (gid, limit)
    ).fetchall()
    
    # Get trending terms (last 7 days)
    trending = db.execute(
        """SELECT term, COUNT(*) as recent_count
           FROM messages
           WHERE guild_id = ? AND created_at >= ?
           GROUP BY term
           ORDER BY recent_count DESC
           LIMIT 10""",
        (gid, week_ago)
    ).fetchall()
    
    # Prepare chart data
    labels = [r["term"] for r in top[:10]]
    counts = [r["total_count"] for r in top[:10]]
    
    tpl = """
    {% extends '_base.html' %}
    {% block content %}
    <!-- Stats Cards -->
    <div class="row mb-4">
      <div class="col-lg-3 col-md-6 mb-3">
        <div class="stat-card">
          <div class="stat-number">{{ overview['terms'] or 0 }}</div>
          <div class="stat-label">Tracked Terms</div>
        </div>
      </div>
      <div class="col-lg-3 col-md-6 mb-3">
        <div class="stat-card">
          <div class="stat-number">{{ "{:,}".format(overview['total_mentions'] or 0) }}</div>
          <div class="stat-label">Total Mentions</div>
        </div>
      </div>
      <div class="col-lg-3 col-md-6 mb-3">
        <div class="stat-card">
          <div class="stat-number">{{ overview['active_users'] or 0 }}</div>
          <div class="stat-label">Active Users</div>
        </div>
      </div>
      <div class="col-lg-3 col-md-6 mb-3">
        <div class="stat-card">
          <div class="stat-number">{{ recent_stats['recent_mentions'] or 0 }}</div>
          <div class="stat-label">This Week</div>
        </div>
      </div>
    </div>
    
    <div class="row">
      <!-- Chart Section -->
      {% if labels %}
      <div class="col-lg-8 mb-4">
        <div class="card">
          <div class="card-header">
            <h5 class="mb-0"><i class="bi bi-bar-chart me-2"></i>Top Terms</h5>
          </div>
          <div class="card-body">
            <canvas id="termsChart" style="max-height: 400px;"></canvas>
          </div>
        </div>
      </div>
      {% endif %}
      
      <!-- Trending Terms -->
      <div class="col-lg-4 mb-4">
        <div class="card">
          <div class="card-header">
            <h5 class="mb-0"><i class="bi bi-trending-up me-2"></i>Trending (7 days)</h5>
          </div>
          <div class="card-body">
            {% if trending %}
              {% for term in trending %}
              <div class="d-flex justify-content-between align-items-center mb-2">
                <code>{{ term['term'] }}</code>
                <span class="badge bg-success">{{ term['recent_count'] }}</span>
              </div>
              {% endfor %}
            {% else %}
              <p class="text-muted text-center">No recent activity</p>
            {% endif %}
          </div>
        </div>
      </div>
    </div>
    
    <!-- Terms Table -->
    {% if top %}
    <div class="card">
      <div class="card-header d-flex justify-content-between align-items-center">
        <h5 class="mb-0"><i class="bi bi-list me-2"></i>All Terms</h5>
        <div>
          <a class="btn btn-outline-primary btn-sm me-2" href="{{ url_for('ui_search', gid=gid) }}">
            <i class="bi bi-search me-1"></i>Search Messages
          </a>
          <a class="btn btn-primary btn-sm" href="{{ url_for('ui_guild_analytics', gid=gid) }}">
            <i class="bi bi-graph-up me-1"></i>Analytics
          </a>
        </div>
      </div>
      <div class="card-body p-0">
        <div class="table-responsive">
          <table class="table table-hover mb-0">
            <thead>
              <tr>
                <th>#</th>
                <th>Term</th>
                <th>Category</th>
                <th class="text-end">Mentions</th>
                <th class="text-end">Users</th>
                <th class="text-center">Popularity</th>
                <th></th>
              </tr>
            </thead>
            <tbody>
              {% for r in top %}
              {% set max_count = top[0]['total_count'] if top else 1 %}
              {% set percentage = (r['total_count'] / max_count * 100) if max_count > 0 else 0 %}
              <tr>
                <td>{{ loop.index }}</td>
                <td><code>{{ r['term'] }}</code></td>
                <td>
                  {% if r['category_name'] %}
                    <span class="badge bg-secondary">{{ r['category_name'] }}</span>
                  {% else %}
                    <small class="text-muted">Uncategorized</small>
                  {% endif %}
                </td>
                <td class="text-end"><strong>{{ "{:,}".format(r['total_count']) }}</strong></td>
                <td class="text-end">{{ r['unique_users'] or 0 }}</td>
                <td class="text-center">
                  <div class="trend-indicator">
                    <div class="trend-bar" style="width: {{ percentage }}%"></div>
                  </div>
                </td>
                <td class="text-end">
                  <a class="btn btn-primary btn-sm" href="{{ url_for('ui_term', gid=gid, term=r['term']) }}">
                    <i class="bi bi-people me-1"></i>Users
                  </a>
                </td>
              </tr>
              {% endfor %}
            </tbody>
          </table>
        </div>
      </div>
    </div>
    {% else %}
    <div class="card">
      <div class="card-body text-center py-5">
        <i class="bi bi-tags display-1 text-muted"></i>
        <h5 class="mt-3 text-muted">No terms tracked yet</h5>
        <p class="text-muted">Terms will appear here once the bot starts tracking activity.</p>
      </div>
    </div>
    {% endif %}

    <script>
    {% if labels %}
    const ctx = document.getElementById('termsChart');
    const data = {
      labels: {{ labels|tojson }},
      datasets: [{
        label: 'Mentions',
        data: {{ counts|tojson }},
        backgroundColor: 'rgba(88, 101, 242, 0.8)',
        borderColor: 'rgba(88, 101, 242, 1)',
        borderWidth: 2,
        borderRadius: 8,
        borderSkipped: false,
      }]
    };
    
    new Chart(ctx, {
      type: 'bar',
      data: data,
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { display: false },
          tooltip: {
            backgroundColor: 'rgba(0, 0, 0, 0.8)',
            titleColor: 'white',
            bodyColor: 'white',
            cornerRadius: 8,
            callbacks: {
              label: function(context) {
                return `${context.parsed.y.toLocaleString()} mentions`;
              }
            }
          }
        },
        scales: {
          y: {
            beginAtZero: true,
            grid: { color: 'rgba(0, 0, 0, 0.1)' },
            ticks: {
              callback: function(value) {
                return value >= 1000 ? (value/1000).toFixed(1) + 'K' : value;
              }
            }
          },
          x: {
            grid: { display: false },
            ticks: {
              maxRotation: 45,
              minRotation: 45
            }
          }
        }
      }
    });
    {% endif %}
    </script>
    {% endblock %}
    """
    return render_template_string(
        tpl,
        gid=gid,
        overview=overview,
        recent_stats=recent_stats,
        top=top,
        trending=trending,
        labels=labels,
        counts=counts,
        title=f"Server {gid}",
        db_path=DB_PATH,
    )

@app.get("/ui/guild/<int:gid>/term/<term>")
def ui_term(gid: int, term: str):
    term = term.lower()
    db = get_db()
    
    # Get term stats
    term_stats = db.execute(
        """SELECT tm.total_count, tm.last_mentioned, tm.last_user, tca.category_name
           FROM term_meta tm
           LEFT JOIN term_category_assignments tca ON tm.guild_id = tca.guild_id AND tm.term = tca.term
           WHERE tm.guild_id = ? AND tm.term = ?""",
        (gid, term)
    ).fetchone()
    
    if not term_stats:
        return render_template_string("""
        {% extends '_base.html' %}
        {% block content %}
        <div class="card">
          <div class="card-body text-center py-5">
            <i class="bi bi-exclamation-triangle display-1 text-warning"></i>
            <h5 class="mt-3">Term not found</h5>
            <p class="text-muted">The term "{{ term }}" is not being tracked in this server.</p>
            <a href="{{ url_for('ui_guild', gid=gid) }}" class="btn btn-primary">Back to Server</a>
          </div>
        </div>
        {% endblock %}
        """, gid=gid, term=term, title="Term Not Found", db_path=DB_PATH)
    
    # Get user leaderboard
    users = db.execute(
        """SELECT user_id, user_name, count, last_seen
           FROM hits
           WHERE guild_id = ? AND term = ?
           ORDER BY count DESC
           LIMIT 50""",
        (gid, term)
    ).fetchall()
    
    # Get recent activity (last 30 days)
    month_ago = (datetime.now() - timedelta(days=30)).isoformat()
    daily_activity = db.execute(
        """SELECT DATE(created_at) as date, COUNT(*) as mentions
           FROM messages
           WHERE guild_id = ? AND term = ? AND created_at >= ?
           GROUP BY DATE(created_at)
           ORDER BY date""",
        (gid, term, month_ago)
    ).fetchall()
    
    # Prepare chart data
    activity_dates = [r['date'] for r in daily_activity]
    activity_counts = [r['mentions'] for r in daily_activity]
    
    tpl = """
    {% extends '_base.html' %}
    {% block content %}
    <div class="d-flex justify-content-between align-items-center mb-4">
      <div>
        <h1 class="display-6">
          <code>{{ term }}</code>
          {% if term_stats['category_name'] %}
            <span class="badge bg-secondary ms-2">{{ term_stats['category_name'] }}</span>
          {% endif %}
        </h1>
        <p class="text-muted mb-0">
          in Server <span class="badge-custom">{{ gid }}</span>
        </p>
      </div>
      <div>
        <a class="btn btn-outline-secondary me-2" href="{{ url_for('ui_guild', gid=gid) }}">
          <i class="bi bi-arrow-left me-1"></i>Back to Server
        </a>
        <a class="btn btn-primary" href="{{ url_for('ui_search', gid=gid, q=term) }}">
          <i class="bi bi-search me-1"></i>Search Messages
        </a>
      </div>
    </div>

    <!-- Stats Cards -->
    <div class="row mb-4">
      <div class="col-md-3">
        <div class="stat-card">
          <div class="stat-number">{{ "{:,}".format(term_stats['total_count']) }}</div>
          <div class="stat-label">Total Mentions</div>
        </div>
      </div>
      <div class="col-md-3">
        <div class="stat-card">
          <div class="stat-number">{{ users|length }}</div>
          <div class="stat-label">Active Users</div>
        </div>
      </div>
      <div class="col-md-3">
        <div class="stat-card">
          <div class="stat-number">{{ activity_counts|sum if activity_counts else 0 }}</div>
          <div class="stat-label">Last 30 Days</div>
        </div>
      </div>
      <div class="col-md-3">
        <div class="stat-card">
          <div class="stat-number">{{ (term_stats['total_count'] / users|length)|round if users else 0 }}</div>
          <div class="stat-label">Avg per User</div>
        </div>
      </div>
    </div>

    <div class="row">
      <!-- Activity Chart -->
      {% if activity_dates %}
      <div class="col-lg-8 mb-4">
        <div class="card">
          <div class="card-header">
            <h5 class="mb-0"><i class="bi bi-graph-up me-2"></i>Activity Over Time</h5>
          </div>
          <div class="card-body">
            <canvas id="activityChart" style="max-height: 300px;"></canvas>
          </div>
        </div>
      </div>
      {% endif %}
      
      <!-- Top User -->
      {% if users %}
      <div class="col-lg-4 mb-4">
        <div class="card">
          <div class="card-header">
            <h5 class="mb-0"><i class="bi bi-trophy me-2"></i>Top User</h5>
          </div>
          <div class="card-body text-center">
            <div class="mb-3">
              <i class="bi bi-person-circle display-4 text-primary"></i>
            </div>
            <h5>{{ users[0]['user_name'] }}</h5>
            <p class="text-muted mb-2">{{ "{:,}".format(users[0]['count']) }} mentions</p>
            {% if users[0]['last_seen'] %}
            <small class="text-muted">Last seen: {{ users[0]['last_seen'][:10] }}</small>
            {% endif %}
          </div>
        </div>
      </div>
      {% endif %}
    </div>

    <!-- User Leaderboard -->
    {% if users %}
    <div class="card">
      <div class="card-header">
        <h5 class="mb-0"><i class="bi bi-people me-2"></i>User Leaderboard</h5>
      </div>
      <div class="card-body p-0">
        <div class="table-responsive">
          <table class="table table-hover mb-0">
            <thead>
              <tr>
                <th>Rank</th>
                <th>User</th>
                <th class="text-end">Mentions</th>
                <th class="text-center">Share</th>
                <th>Last Seen</th>
              </tr>
            </thead>
            <tbody>
              {% for user in users %}
              {% set percentage = (user['count'] / term_stats['total_count'] * 100) if term_stats['total_count'] > 0 else 0 %}
              <tr>
                <td>
                  {% if loop.index <= 3 %}
                    <span class="achievement-badge">
                      {% if loop.index == 1 %}🥇
                      {% elif loop.index == 2 %}🥈
                      {% else %}🥉
                      {% endif %}
                      {{ loop.index }}
                    </span>
                  {% else %}
                    {{ loop.index }}
                  {% endif %}
                </td>
                <td><strong>{{ user['user_name'] }}</strong></td>
                <td class="text-end"><strong>{{ "{:,}".format(user['count']) }}</strong></td>
                <td class="text-center">
                  <div class="trend-indicator">
                    <div class="trend-bar" style="width: {{ percentage }}%"></div>
                  </div>
                  <small class="text-muted">{{ "%.1f"|format(percentage) }}%</small>
                </td>
                <td>
                  {% if user['last_seen'] %}
                    <small class="text-muted">{{ user['last_seen'][:10] }}</small>
                  {% else %}
                    <small class="text-muted">Never</small>
                  {% endif %}
                </td>
              </tr>
              {% endfor %}
            </tbody>
          </table>
        </div>
      </div>
    </div>
    {% else %}
    <div class="card">
      <div class="card-body text-center py-5">
        <i class="bi bi-people display-1 text-muted"></i>
        <h5 class="mt-3 text-muted">No user data yet</h5>
        <p class="text-muted">User statistics will appear here once people start mentioning this term.</p>
      </div>
    </div>
    {% endif %}

    <script>
    {% if activity_dates %}
    const activityCtx = document.getElementById('activityChart');
    new Chart(activityCtx, {
      type: 'line',
      data: {
        labels: {{ activity_dates|tojson }},
        datasets: [{
          label: 'Daily Mentions',
          data: {{ activity_counts|tojson }},
          borderColor: 'rgba(88, 101, 242, 1)',
          backgroundColor: 'rgba(88, 101, 242, 0.1)',
          borderWidth: 3,
          fill: true,
          tension: 0.4,
          pointBackgroundColor: 'rgba(88, 101, 242, 1)',
          pointBorderColor: 'white',
          pointBorderWidth: 2,
          pointRadius: 4,
          pointHoverRadius: 6
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { display: false },
          tooltip: {
            backgroundColor: 'rgba(0, 0, 0, 0.8)',
            titleColor: 'white',
            bodyColor: 'white',
            cornerRadius: 8
          }
        },
        scales: {
          y: {
            beginAtZero: true,
            grid: { color: 'rgba(0, 0, 0, 0.1)' }
          },
          x: {
            grid: { display: false },
            ticks: {
              callback: function(value, index) {
                const date = this.getLabelForValue(value);
                return new Date(date).toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
              }
            }
          }
        }
      }
    });
    {% endif %}
    </script>
    {% endblock %}
    """
    
    return render_template_string(
        tpl,
        gid=gid,
        term=term,
        term_stats=term_stats,
        users=users,
        activity_dates=activity_dates,
        activity_counts=activity_counts,
        title=f"{term} • Server {gid}",
        db_path=DB_PATH
    )

@app.get("/ui/search")
def ui_search():
    q = (request.args.get("q") or "").strip()
    gid = request.args.get("gid")
    limit = min(int(request.args.get("limit", 50)), 500)
    rows = []
    
    if q:
        like = f"%{q}%"
        db = get_db()
        if gid:
            rows = db.execute(
                """
              SELECT guild_id, channel_id, user_name, term,
                     substr(content,1,300) AS snippet, created_at
              FROM messages
              WHERE guild_id = ? AND content LIKE ?
              ORDER BY id DESC
              LIMIT ?
            """,
                (int(gid), like, limit),
            ).fetchall()
        else:
            rows = db.execute(
                """
              SELECT guild_id, channel_id, user_name, term,
                     substr(content,1,300) AS snippet, created_at
              FROM messages
              WHERE content LIKE ?
              ORDER BY id DESC
              LIMIT ?
            """,
                (like, limit),
            ).fetchall()
    
    tpl = """
    {% extends '_base.html' %}
    {% block content %}
    <div class="search-container mb-4">
      <h2 class="mb-4">
        <i class="bi bi-search me-2"></i>Search Messages
      </h2>
      
      <form method="get" class="row g-3">
        <div class="col-md-6">
          <label for="searchQuery" class="form-label">Search Query</label>
          <input 
            type="text" 
            class="form-control" 
            id="searchQuery"
            name="q" 
            placeholder="Enter text to search for..." 
            value="{{ q }}" 
            required
            autocomplete="off"
          >
        </div>
        <div class="col-md-3">
          <label for="guildId" class="form-label">Server ID (Optional)</label>
          <input 
            type="number" 
            class="form-control" 
            id="guildId"
            name="gid" 
            placeholder="Filter by server..." 
            value="{{ gid or '' }}"
          >
        </div>
        <div class="col-md-2">
          <label for="resultLimit" class="form-label">Results</label>
          <select class="form-select" id="resultLimit" name="limit">
            <option value="25" {{ 'selected' if limit == 25 else '' }}>25</option>
            <option value="50" {{ 'selected' if limit == 50 else '' }}>50</option>
            <option value="100" {{ 'selected' if limit == 100 else '' }}>100</option>
            <option value="250" {{ 'selected' if limit == 250 else '' }}>250</option>
            <option value="500" {{ 'selected' if limit == 500 else '' }}>500</option>
          </select>
        </div>
        <div class="col-md-1 d-flex align-items-end">
          <button type="submit" class="btn btn-primary w-100">
            <i class="bi bi-search"></i>
          </button>
        </div>
      </form>
    </div>

    {% if q %}
      {% if rows %}
      <div class="card">
        <div class="card-header d-flex justify-content-between align-items-center">
          <h5 class="mb-0">
            <i class="bi bi-chat-dots me-2"></i>
            Search Results for "{{ q }}"
          </h5>
          <span class="badge bg-primary">{{ rows|length }} result{{ 's' if rows|length != 1 else '' }}</span>
        </div>
        <div class="card-body p-0">
          <div class="list-group list-group-flush">
            {% for r in rows %}
            <div class="list-group-item">
              <div class="d-flex w-100 justify-content-between align-items-start mb-2">
                <div class="d-flex align-items-center">
                  <span class="badge-custom me-2">{{ r['guild_id'] }}</span>
                  <strong>{{ r['user_name'] }}</strong>
                  <span class="badge bg-secondary ms-2">{{ r['term'] }}</span>
                </div>
                <small class="text-muted">{{ r['created_at'][:19] }}</small>
              </div>
              <p class="mb-0">{{ r['snippet'] }}</p>
              {% if r['snippet']|length >= 299 %}
                <small class="text-muted">...</small>
              {% endif %}
            </div>
            {% endfor %}
          </div>
        </div>
        {% if rows|length >= limit %}
        <div class="card-footer text-center">
          <small class="text-muted">
            Showing first {{ limit }} results. Use more specific search terms for better results.
          </small>
        </div>
        {% endif %}
      </div>
      {% else %}
      <div class="card">
        <div class="card-body text-center py-5">
          <i class="bi bi-search display-1 text-muted"></i>
          <h5 class="mt-3 text-muted">No results found</h5>
          <p class="text-muted">No messages found containing "{{ q }}".</p>
          <small class="text-muted">Try different search terms or check the server ID.</small>
        </div>
      </div>
      {% endif %}
    {% else %}
    <div class="card">
      <div class="card-body text-center py-5">
        <i class="bi bi-search display-1 text-primary"></i>
        <h5 class="mt-3">Search Term Messages</h5>
        <p class="text-muted mb-0">
          Search through all tracked term mentions across all servers or filter by specific server.
        </p>
      </div>
    </div>
    {% endif %}
    {% endblock %}
    """
    return render_template_string(
        tpl,
        q=q,
        gid=gid,
        limit=limit,
        rows=rows,
        title="Search",
        db_path=DB_PATH,
    )

@app.get("/ui/analytics")
def ui_analytics():
    """Global analytics dashboard"""
    db = get_db()
    
    # Global stats
    global_stats = db.execute(
        """SELECT COUNT(DISTINCT guild_id) as guilds,
                  COUNT(DISTINCT term) as terms,
                  SUM(total_count) as total_mentions
           FROM term_meta
           WHERE guild_id != 0""").fetchone()
    
    # Most active guilds
    top_guilds = db.execute(
        """SELECT guild_id, SUM(total_count) as mentions, COUNT(DISTINCT term) as terms
           FROM term_meta 
           WHERE guild_id != 0
           GROUP BY guild_id 
           ORDER BY mentions DESC 
           LIMIT 10""").fetchall()
    
    # Most popular terms globally
    top_terms = db.execute(
        """SELECT term, SUM(total_count) as total_mentions, COUNT(DISTINCT guild_id) as servers
           FROM term_meta 
           WHERE guild_id != 0
           GROUP BY term 
           ORDER BY total_mentions DESC 
           LIMIT 15""").fetchall()
    
    # Recent activity trend (last 30 days)
    month_ago = (datetime.now() - timedelta(days=30)).isoformat()
    daily_activity = db.execute(
        """SELECT DATE(created_at) as date, COUNT(*) as mentions
           FROM messages 
           WHERE created_at >= ?
           GROUP BY DATE(created_at) 
           ORDER BY date""", (month_ago,)).fetchall()
    
    activity_dates = [r['date'] for r in daily_activity]
    activity_counts = [r['mentions'] for r in daily_activity]
    
    tpl = """
    {% extends '_base.html' %}
    {% block content %}
    <div class="d-flex justify-content-between align-items-center mb-4">
      <div>
        <h1 class="display-6">
          <i class="bi bi-graph-up me-3"></i>Global Analytics
        </h1>
        <p class="text-muted mb-0">Insights across all servers</p>
      </div>
    </div>

    <!-- Global Stats -->
    <div class="row mb-4">
      <div class="col-md-4">
        <div class="stat-card">
          <div class="stat-number">{{ global_stats['guilds'] or 0 }}</div>
          <div class="stat-label">Active Servers</div>
        </div>
      </div>
      <div class="col-md-4">
        <div class="stat-card">
          <div class="stat-number">{{ "{:,}".format(global_stats['terms'] or 0) }}</div>
          <div class="stat-label">Unique Terms</div>
        </div>
      </div>
      <div class="col-md-4">
        <div class="stat-card">
          <div class="stat-number">{{ "{:,}".format(global_stats['total_mentions'] or 0) }}</div>
          <div class="stat-label">Total Mentions</div>
        </div>
      </div>
    </div>

    <div class="row">
      <!-- Activity Chart -->
      {% if activity_dates %}
      <div class="col-lg-8 mb-4">
        <div class="card">
          <div class="card-header">
            <h5 class="mb-0">
              <i class="bi bi-activity me-2"></i>Activity Trend (30 days)
            </h5>
          </div>
          <div class="card-body">
            <canvas id="activityChart" style="max-height: 300px;"></canvas>
          </div>
        </div>
      </div>
      {% endif %}
      
      <!-- Top Servers -->
      <div class="col-lg-4 mb-4">
        <div class="card">
          <div class="card-header">
            <h5 class="mb-0">
              <i class="bi bi-servers me-2"></i>Most Active Servers
            </h5>
          </div>
          <div class="card-body">
            {% for guild in top_guilds[:5] %}
            <div class="d-flex justify-content-between align-items-center mb-2">
              <div>
                <span class="badge-custom">{{ guild['guild_id'] }}</span>
                <small class="text-muted ms-1">({{ guild['terms'] }} terms)</small>
              </div>
              <span class="badge bg-primary">{{ "{:,}".format(guild['mentions']) }}</span>
            </div>
            {% endfor %}
          </div>
        </div>
      </div>
    </div>

    <!-- Popular Terms -->
    {% if top_terms %}
    <div class="card">
      <div class="card-header">
        <h5 class="mb-0">
          <i class="bi bi-tags me-2"></i>Most Popular Terms Globally
        </h5>
      </div>
      <div class="card-body p-0">
        <div class="table-responsive">
          <table class="table table-hover mb-0">
            <thead>
              <tr>
                <th>Rank</th>
                <th>Term</th>
                <th class="text-end">Total Mentions</th>
                <th class="text-end">Servers</th>
                <th class="text-center">Popularity</th>
              </tr>
            </thead>
            <tbody>
              {% for term in top_terms %}
              {% set max_mentions = top_terms[0]['total_mentions'] if top_terms else 1 %}
              {% set percentage = (term['total_mentions'] / max_mentions * 100) if max_mentions > 0 else 0 %}
              <tr>
                <td>
                  {% if loop.index <= 3 %}
                    <span class="achievement-badge">
                      {% if loop.index == 1 %}🥇
                      {% elif loop.index == 2 %}🥈
                      {% else %}🥉
                      {% endif %}
                      {{ loop.index }}
                    </span>
                  {% else %}
                    {{ loop.index }}
                  {% endif %}
                </td>
                <td><code>{{ term['term'] }}</code></td>
                <td class="text-end">
                  <strong>{{ "{:,}".format(term['total_mentions']) }}</strong>
                </td>
                <td class="text-end">
                  <span class="badge bg-secondary">{{ term['servers'] }}</span>
                </td>
                <td class="text-center">
                  <div class="trend-indicator">
                    <div class="trend-bar" style="width: {{ percentage }}%"></div>
                  </div>
                </td>
              </tr>
              {% endfor %}
            </tbody>
          </table>
        </div>
      </div>
    </div>
    {% endif %}

    <script>
    {% if activity_dates %}
    const activityCtx = document.getElementById('activityChart');
    new Chart(activityCtx, {
      type: 'line',
      data: {
        labels: {{ activity_dates|tojson }},
        datasets: [{
          label: 'Daily Mentions',
          data: {{ activity_counts|tojson }},
          borderColor: 'rgba(88, 101, 242, 1)',
          backgroundColor: 'rgba(88, 101, 242, 0.1)',
          borderWidth: 3,
          fill: true,
          tension: 0.4,
          pointBackgroundColor: 'rgba(88, 101, 242, 1)',
          pointBorderColor: 'white',
          pointBorderWidth: 2,
          pointRadius: 4,
          pointHoverRadius: 6
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { display: false },
          tooltip: {
            backgroundColor: 'rgba(0, 0, 0, 0.8)',
            titleColor: 'white',
            bodyColor: 'white',
            cornerRadius: 8
          }
        },
        scales: {
          y: {
            beginAtZero: true,
            grid: { color: 'rgba(0, 0, 0, 0.1)' }
          },
          x: {
            grid: { display: false },
            ticks: {
              callback: function(value, index) {
                const date = this.getLabelForValue(value);
                return new Date(date).toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
              }
            }
          }
        }
      }
    });
    {% endif %}
    </script>
    {% endblock %}
    """
    
    return render_template_string(
        tpl,
        global_stats=global_stats,
        top_guilds=top_guilds,
        top_terms=top_terms,
        activity_dates=activity_dates,
        activity_counts=activity_counts,
        title="Global Analytics",
        db_path=DB_PATH
    )

@app.get("/ui/guild/<int:gid>/analytics")
def ui_guild_analytics(gid: int):
    """Detailed analytics for a specific guild"""
    db = get_db()
    
    # Get timeframe data for trends
    timeframes = {
        'week': 7,
        'month': 30,
        'quarter': 90
    }
    
    trends_data = {}
    for period, days in timeframes.items():
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
        period_data = db.execute(
            """SELECT COUNT(*) as mentions, COUNT(DISTINCT user_id) as users, COUNT(DISTINCT term) as terms
               FROM messages WHERE guild_id = ? AND created_at >= ?""",
            (gid, cutoff)
        ).fetchone()
        trends_data[period] = dict(period_data)
    
    # Category breakdown
    category_stats = db.execute(
        """SELECT COALESCE(tca.category_name, 'Uncategorized') as category,
                  COUNT(DISTINCT tm.term) as terms,
                  SUM(tm.total_count) as mentions
           FROM term_meta tm
           LEFT JOIN term_category_assignments tca ON tm.guild_id = tca.guild_id AND tm.term = tca.term
           WHERE tm.guild_id = ?
           GROUP BY COALESCE(tca.category_name, 'Uncategorized')
           ORDER BY mentions DESC""",
        (gid,)
    ).fetchall()
    
    # User engagement levels
    user_engagement = db.execute(
        """SELECT 
             CASE 
               WHEN SUM(count) >= 100 THEN 'High'
               WHEN SUM(count) >= 20 THEN 'Medium'
               ELSE 'Low'
             END as engagement_level,
             COUNT(*) as user_count
           FROM hits WHERE guild_id = ?
           GROUP BY 
             CASE 
               WHEN SUM(count) >= 100 THEN 'High'
               WHEN SUM(count) >= 20 THEN 'Medium'
               ELSE 'Low'
             END""",
        (gid,)
    ).fetchall()
    
    # Peak activity hours (from messages)
    hourly_activity = db.execute(
        """SELECT strftime('%H', created_at) as hour, COUNT(*) as mentions
           FROM messages WHERE guild_id = ?
           GROUP BY strftime('%H', created_at)
           ORDER BY hour""",
        (gid,)
    ).fetchall()
    
    hours = [int(r['hour']) for r in hourly_activity]
    hourly_counts = [r['mentions'] for r in hourly_activity]
    
    tpl = """
    {% extends '_base.html' %}
    {% block content %}
    <div class="d-flex justify-content-between align-items-center mb-4">
      <div>
        <h1 class="display-6">
          <i class="bi bi-graph-up me-3"></i>Server Analytics
        </h1>
        <p class="text-muted mb-0">Server <span class="badge-custom">{{ gid }}</span></p>
      </div>
      <a href="{{ url_for('ui_guild', gid=gid) }}" class="btn btn-outline-secondary">
        <i class="bi bi-arrow-left me-1"></i>Back to Server
      </a>
    </div>

    <!-- Trend Cards -->
    <div class="row mb-4">
      <div class="col-lg-4 mb-3">
        <div class="card">
          <div class="card-header">
            <h6 class="mb-0"><i class="bi bi-calendar-week me-2"></i>Last 7 Days</h6>
          </div>
          <div class="card-body">
            <div class="row text-center">
              <div class="col-4">
                <div class="stat-number text-primary">{{ "{:,}".format(trends_data['week']['mentions'] or 0) }}</div>
                <small class="stat-label">Mentions</small>
              </div>
              <div class="col-4">
                <div class="stat-number text-success">{{ trends_data['week']['users'] or 0 }}</div>
                <small class="stat-label">Users</small>
              </div>
              <div class="col-4">
                <div class="stat-number text-info">{{ trends_data['week']['terms'] or 0 }}</div>
                <small class="stat-label">Terms</small>
              </div>
            </div>
          </div>
        </div>
      </div>
      <div class="col-lg-4 mb-3">
        <div class="card">
          <div class="card-header">
            <h6 class="mb-0"><i class="bi bi-calendar-month me-2"></i>Last 30 Days</h6>
          </div>
          <div class="card-body">
            <div class="row text-center">
              <div class="col-4">
                <div class="stat-number text-primary">{{ "{:,}".format(trends_data['month']['mentions'] or 0) }}</div>
                <small class="stat-label">Mentions</small>
              </div>
              <div class="col-4">
                <div class="stat-number text-success">{{ trends_data['month']['users'] or 0 }}</div>
                <small class="stat-label">Users</small>
              </div>
              <div class="col-4">
                <div class="stat-number text-info">{{ trends_data['month']['terms'] or 0 }}</div>
                <small class="stat-label">Terms</small>
              </div>
            </div>
          </div>
        </div>
      </div>
      <div class="col-lg-4 mb-3">
        <div class="card">
          <div class="card-header">
            <h6 class="mb-0"><i class="bi bi-calendar3 me-2"></i>Last 90 Days</h6>
          </div>
          <div class="card-body">
            <div class="row text-center">
              <div class="col-4">
                <div class="stat-number text-primary">{{ "{:,}".format(trends_data['quarter']['mentions'] or 0) }}</div>
                <small class="stat-label">Mentions</small>
              </div>
              <div class="col-4">
                <div class="stat-number text-success">{{ trends_data['quarter']['users'] or 0 }}</div>
                <small class="stat-label">Users</small>
              </div>
              <div class="col-4">
                <div class="stat-number text-info">{{ trends_data['quarter']['terms'] or 0 }}</div>
                <small class="stat-label">Terms</small>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>

    <div class="row">
      <!-- Category Breakdown -->
      {% if category_stats %}
      <div class="col-lg-6 mb-4">
        <div class="card">
          <div class="card-header">
            <h5 class="mb-0"><i class="bi bi-pie-chart me-2"></i>Categories</h5>
          </div>
          <div class="card-body">
            <canvas id="categoryChart" style="max-height: 300px;"></canvas>
          </div>
        </div>
      </div>
      {% endif %}
      
      <!-- User Engagement -->
      {% if user_engagement %}
      <div class="col-lg-6 mb-4">
        <div class="card">
          <div class="card-header">
            <h5 class="mb-0"><i class="bi bi-people me-2"></i>User Engagement</h5>
          </div>
          <div class="card-body">
            {% for engagement in user_engagement %}
            <div class="d-flex justify-content-between align-items-center mb-3">
              <div class="d-flex align-items-center">
                {% if engagement['engagement_level'] == 'High' %}
                  <i class="bi bi-fire text-danger me-2"></i>
                  <span class="text-danger fw-bold">High Activity</span>
                {% elif engagement['engagement_level'] == 'Medium' %}
                  <i class="bi bi-lightning text-warning me-2"></i>
                  <span class="text-warning fw-bold">Medium Activity</span>
                {% else %}
                  <i class="bi bi-circle text-muted me-2"></i>
                  <span class="text-muted">Low Activity</span>
                {% endif %}
                <small class="text-muted ms-2">
                  {% if engagement['engagement_level'] == 'High' %}
                    (100+ mentions)
                  {% elif engagement['engagement_level'] == 'Medium' %}
                    (20-99 mentions)
                  {% else %}
                    (< 20 mentions)
                  {% endif %}
                </small>
              </div>
              <span class="badge bg-primary">{{ engagement['user_count'] }} users</span>
            </div>
            {% endfor %}
          </div>
        </div>
      </div>
      {% endif %}
    </div>

    <!-- Peak Hours -->
    {% if hourly_activity %}
    <div class="card mb-4">
      <div class="card-header">
        <h5 class="mb-0"><i class="bi bi-clock me-2"></i>Peak Activity Hours</h5>
      </div>
      <div class="card-body">
        <canvas id="hourlyChart" style="max-height: 250px;"></canvas>
      </div>
    </div>
    {% endif %}

    <!-- Category Details -->
    {% if category_stats %}
    <div class="card">
      <div class="card-header">
        <h5 class="mb-0"><i class="bi bi-folder2 me-2"></i>Category Breakdown</h5>
      </div>
      <div class="card-body p-0">
        <div class="table-responsive">
          <table class="table table-hover mb-0">
            <thead>
              <tr>
                <th>Category</th>
                <th class="text-end">Terms</th>
                <th class="text-end">Total Mentions</th>
                <th class="text-center">Share</th>
              </tr>
            </thead>
            <tbody>
              {% set total_mentions = category_stats|sum(attribute='mentions') %}
              {% for cat in category_stats %}
              {% set percentage = (cat['mentions'] / total_mentions * 100) if total_mentions > 0 else 0 %}
              <tr>
                <td>
                  {% if cat['category'] == 'Uncategorized' %}
                    <span class="text-muted">
                      <i class="bi bi-question-circle me-1"></i>{{ cat['category'] }}
                    </span>
                  {% else %}
                    <span class="badge bg-secondary">{{ cat['category'] }}</span>
                  {% endif %}
                </td>
                <td class="text-end">{{ cat['terms'] }}</td>
                <td class="text-end"><strong>{{ "{:,}".format(cat['mentions']) }}</strong></td>
                <td class="text-center">
                  <div class="trend-indicator">
                    <div class="trend-bar" style="width: {{ percentage }}%"></div>
                  </div>
                  <small class="text-muted">{{ "%.1f"|format(percentage) }}%</small>
                </td>
              </tr>
              {% endfor %}
            </tbody>
          </table>
        </div>
      </div>
    </div>
    {% endif %}

    <script>
    // Category pie chart
    {% if category_stats %}
    const categoryCtx = document.getElementById('categoryChart');
    const categoryData = {
      labels: {{ category_stats|map(attribute='category')|list|tojson }},
      datasets: [{
        data: {{ category_stats|map(attribute='mentions')|list|tojson }},
        backgroundColor: [
          'rgba(88, 101, 242, 0.8)',
          'rgba(34, 197, 94, 0.8)',
          'rgba(249, 115, 22, 0.8)',
          'rgba(239, 68, 68, 0.8)',
          'rgba(168, 85, 247, 0.8)',
          'rgba(14, 165, 233, 0.8)',
          'rgba(236, 72, 153, 0.8)',
          'rgba(132, 204, 22, 0.8)',
        ],
        borderWidth: 2,
        borderColor: 'white'
      }]
    };
    
    new Chart(categoryCtx, {
      type: 'doughnut',
      data: categoryData,
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: {
            position: 'bottom',
            labels: {
              padding: 20,
              usePointStyle: true
            }
          },
          tooltip: {
            backgroundColor: 'rgba(0, 0, 0, 0.8)',
            titleColor: 'white',
            bodyColor: 'white',
            cornerRadius: 8,
            callbacks: {
              label: function(context) {
                const total = context.dataset.data.reduce((a, b) => a + b, 0);
                const percentage = ((context.parsed / total) * 100).toFixed(1);
                return `${context.label}: ${context.parsed.toLocaleString()} (${percentage}%)`;
              }
            }
          }
        }
      }
    });
    {% endif %}

    // Hourly activity chart
    {% if hourly_activity %}
    const hourlyCtx = document.getElementById('hourlyChart');
    const hourlyData = {
      labels: Array.from({length: 24}, (_, i) => `${i}:00`),
      datasets: [{
        label: 'Messages per Hour',
        data: {{ hourly_counts|tojson }},
        backgroundColor: 'rgba(88, 101, 242, 0.2)',
        borderColor: 'rgba(88, 101, 242, 1)',
        borderWidth: 2,
        fill: true,
        tension: 0.4
      }]
    };
    
    new Chart(hourlyCtx, {
      type: 'line',
      data: hourlyData,
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { display: false },
          tooltip: {
            backgroundColor: 'rgba(0, 0, 0, 0.8)',
            titleColor: 'white',
            bodyColor: 'white',
            cornerRadius: 8
          }
        },
        scales: {
          y: {
            beginAtZero: true,
            grid: { color: 'rgba(0, 0, 0, 0.1)' }
          },
          x: {
            grid: { display: false }
          }
        }
      }
    });
    {% endif %}
    </script>
    {% endblock %}
    """
    
    return render_template_string(
        tpl,
        gid=gid,
        trends_data=trends_data,
        category_stats=category_stats,
        user_engagement=user_engagement,
        hourly_activity=hourly_activity,
        hours=hours,
        hourly_counts=hourly_counts,
        title=f"Analytics • Server {gid}",
        db_path=DB_PATH
    )

# Local dev
if __name__ == "__main__":
    app.run(debug=True, port=8000)