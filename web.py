import os, sqlite3
from flask import Flask, jsonify, request, g, abort, render_template_string, redirect, url_for
from jinja2 import ChoiceLoader, DictLoader

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
# JSON API (unchanged behavior)
# ----------------------
@app.get("/healthz")
def health():
    return {"ok": True}

@app.get("/guilds")
def guilds_json():
    db = get_db()
    rows = db.execute(
        """
      SELECT guild_id,
             COUNT(DISTINCT term) AS terms,
             COALESCE(SUM(total_count),0) AS mentions
      FROM term_meta
      GROUP BY guild_id
      ORDER BY guild_id
    """
    ).fetchall()
    return jsonify([dict(r) for r in rows])

@app.get("/guild/<int:gid>/top_terms")
def top_terms_json(gid):
    limit = min(int(request.args.get("limit", 20)), 100)
    db = get_db()
    rows = db.execute(
        """
      SELECT term, total_count
      FROM term_meta
      WHERE guild_id = ?
      ORDER BY total_count DESC
      LIMIT ?
    """,
        (gid, limit),
    ).fetchall()
    return jsonify([dict(r) for r in rows])

@app.get("/guild/<int:gid>/term/<term>/leaderboard")
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

@app.get("/search")
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

# ----------------------
# UI (HTML) endpoints using inline templates
# ----------------------
BASE = """<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>{{ title or 'TermBot' }}</title>
  <link href=\"https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css\" rel=\"stylesheet\" />
  <style>
    body{padding-top:2rem}
    .navbar-brand{font-weight:600}
    .badge-id{font-family:ui-monospace, SFMono-Regular, Menlo, monospace}
    .footer{color:#6c757d; font-size:.9rem; margin-top:3rem}
    .table td, .table th{vertical-align:middle}
    .searchbar{max-width:680px}
  </style>
</head>
<body>
<nav class=\"navbar navbar-expand-lg bg-body-tertiary\">
  <div class=\"container\">
    <a class=\"navbar-brand\" href=\"{{ url_for('home') }}\">TermBot</a>
    <div>
      <a class=\"btn btn-outline-primary me-2\" href=\"{{ url_for('ui_guilds') }}\">Guilds</a>
      <a class=\"btn btn-primary\" href=\"{{ url_for('ui_search') }}\">Search</a>
    </div>
  </div>
</nav>
<main class=\"container\">
  {% block content %}{% endblock %}
  <div class=\"footer\">SQLite: {{ db_path }} · <code>/guilds</code> JSON API available</div>
</main>
<script src=\"https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/js/bootstrap.bundle.min.js\"></script>
<script src=\"https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js\"></script>
<script src=\"https://cdn.jsdelivr.net/npm/dayjs@1/dayjs.min.js\"></script>
</body>
</html>"""

# Register the base template so `{% extends '_base.html' %}` works
app.jinja_loader = ChoiceLoader([app.jinja_loader, DictLoader({'_base.html': BASE})])

@app.get("/")
def home():
    return redirect(url_for("ui_guilds"))

@app.get("/ui/guilds")
def ui_guilds():
    db = get_db()
    rows = db.execute(
        """
      SELECT guild_id,
             COUNT(DISTINCT term) AS terms,
             COALESCE(SUM(total_count),0) AS mentions
      FROM term_meta
      GROUP BY guild_id
      ORDER BY guild_id
    """
    ).fetchall()
    tpl = """
    {% extends '_base.html' %}
    {% block content %}
    <h1 class=\"mb-3\">Guilds</h1>
    <p class=\"text-secondary\">List of guild IDs known to the database (names are not stored).</p>
    <div class=\"table-responsive\">
      <table class=\"table table-hover align-middle\">
        <thead><tr><th>Guild ID</th><th class=\"text-end\">Terms</th><th class=\"text-end\">Mentions</th><th></th></tr></thead>
        <tbody>
        {% for r in rows %}
          <tr>
            <td><span class=\"badge text-bg-light badge-id\">{{ r['guild_id'] }}</span></td>
            <td class=\"text-end\">{{ r['terms'] }}</td>
            <td class=\"text-end\">{{ r['mentions'] }}</td>
            <td class=\"text-end\"><a class=\"btn btn-sm btn-primary\" href=\"{{ url_for('ui_guild', gid=r['guild_id']) }}\">View</a></td>
          </tr>
        {% endfor %}
        </tbody>
      </table>
    </div>
    {% endblock %}
    """
    return render_template_string(tpl, rows=rows, title="Guilds", db_path=DB_PATH)

@app.get("/ui/guild/<int:gid>")
def ui_guild(gid: int):
    db = get_db()
    limit = min(int(request.args.get("limit", 20)), 100)
    top = db.execute(
        """
      SELECT term, total_count
      FROM term_meta
      WHERE guild_id=?
      ORDER BY total_count DESC
      LIMIT ?
    """,
        (gid, limit),
    ).fetchall()
    labels = [r["term"] for r in top]
    counts = [r["total_count"] for r in top]
    tpl = """
    {% extends '_base.html' %}
    {% block content %}
    <div class=\"d-flex justify-content-between align-items-center mb-3\">
      <h1 class=\"mb-0\">Guild <span class=\"badge text-bg-light badge-id\">{{ gid }}</span></h1>
      <a class=\"btn btn-outline-secondary\" href=\"{{ url_for('ui_search', gid=gid) }}\">Search messages</a>
    </div>

    {% if labels %}
      <div class=\"card mb-4\">
        <div class=\"card-body\">
          <h5 class=\"card-title\">Top terms</h5>
          <canvas id=\"chart\"></canvas>
        </div>
      </div>

      <div class=\"table-responsive\">
        <table class=\"table table-striped\">
          <thead><tr><th>#</th><th>Term</th><th class=\"text-end\">Mentions</th><th></th></tr></thead>
          <tbody>
          {% for r in top %}
            <tr>
              <td>{{ loop.index }}</td>
              <td><code>{{ r['term'] }}</code></td>
              <td class=\"text-end\">{{ r['total_count'] }}</td>
              <td class=\"text-end\"><a class=\"btn btn-sm btn-primary\" href=\"{{ url_for('ui_term', gid=gid, term=r['term']) }}\">Leaderboard</a></td>
            </tr>
          {% endfor %}
          </tbody>
        </table>
      </div>
    {% else %}
      <p class=\"text-secondary\">No terms yet for this guild.</p>
    {% endif %}

    <script>
    const ctx = document.getElementById('chart');
    const data = {
      labels: {{ labels|tojson }},
      datasets: [{label: 'Mentions', data: {{ counts|tojson }}}]
    };
    new Chart(ctx, {type: 'bar', data});
    </script>
    {% endblock %}
    """
    return render_template_string(
        tpl,
        gid=gid,
        top=top,
        labels=labels,
        counts=counts,
        title=f"Guild {gid}",
        db_path=DB_PATH,
    )

@app.get("/ui/guild/<int:gid>/term/<term>")
def ui_term(gid: int, term: str):
    term = term.lower()
    db = get_db()
    rows = db.execute(
        """
      SELECT user_id, user_name, count, last_seen
      FROM hits
      WHERE guild_id = ? AND term = ?
      ORDER BY count DESC
      LIMIT 100
    """,
        (gid, term),
    ).fetchall()
    tpl = """
    {% extends '_base.html' %}
    {% block content %}
    <h1>Leaderboard for <code>{{ term }}</code> in guild <span class=\"badge text-bg-light badge-id\">{{ gid }}</span></h1>
    {% if rows %}
      <div class=\"table-responsive\">
        <table class=\"table table-hover\">
          <thead><tr><th>#</th><th>User</th><th class=\"text-end\">Count</th><th>Last seen</th></tr></thead>
          <tbody>
            {% for r in rows %}
              <tr>
                <td>{{ loop.index }}</td>
                <td>{{ r['user_name'] }}</td>
                <td class=\"text-end\">{{ r['count'] }}</td>
                <td><span class=\"text-secondary\">{{ r['last_seen'] or '' }}</span></td>
              </tr>
            {% endfor %}
          </tbody>
        </table>
      </div>
    {% else %}
      <p class=\"text-secondary\">No data yet for this term.</p>
    {% endif %}
    {% endblock %}
    """
    return render_template_string(tpl, gid=gid, term=term, rows=rows, title=f"{term} · {gid}", db_path=DB_PATH)

@app.get("/ui/search")
def ui_search():
    q = (request.args.get("q") or "").strip()
    gid = request.args.get("gid")
    limit = min(int(request.args.get("limit", 100)), 1000)
    rows = []
    if q:
        like = f"%{q}%"
        db = get_db()
        if gid:
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
        else:
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
    tpl = """
    {% extends '_base.html' %}
    {% block content %}
    <h1 class=\"mb-3\">Search</h1>
    <form class=\"row gy-2 gx-2 align-items-center searchbar mb-3\" method=\"get\">
      <div class=\"col-12 col-md-6\">
        <input class=\"form-control\" type=\"text\" name=\"q\" placeholder=\"Search text...\" value=\"{{ q }}\" required>
      </div>
      <div class=\"col-6 col-md-3\">
        <input class=\"form-control\" type=\"number\" name=\"gid\" placeholder=\"Guild ID (optional)\" value=\"{{ gid or '' }}\">
      </div>
      <div class=\"col-6 col-md-2\">
        <input class=\"form-control\" type=\"number\" name=\"limit\" min=\"1\" max=\"1000\" value=\"{{ limit }}\">
      </div>
      <div class=\"col-12 col-md-1\"><button class=\"btn btn-primary w-100\">Go</button></div>
    </form>

    {% if q and rows %}
      <p class=\"text-secondary\">Showing up to {{ limit }} result(s).</p>
      <div class=\"table-responsive\">
        <table class=\"table table-striped\">
          <thead><tr><th>Guild</th><th>User</th><th>Term</th><th>Snippet</th><th>When</th></tr></thead>
          <tbody>
            {% for r in rows %}
              <tr>
                <td><span class=\"badge text-bg-light badge-id\">{{ r['guild_id'] }}</span></td>
                <td>{{ r['user_name'] }}</td>
                <td><code>{{ r['term'] }}</code></td>
                <td>{{ r['snippet'] }}</td>
                <td><span class=\"text-secondary\">{{ r['created_at'] }}</span></td>
              </tr>
            {% endfor %}
          </tbody>
        </table>
      </div>
    {% elif q %}
      <p class=\"text-secondary\">No matches.</p>
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

# Local dev
if __name__ == "__main__":
    app.run(debug=True, port=8000)