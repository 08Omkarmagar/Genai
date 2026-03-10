import os
from datetime import datetime
from dotenv import load_dotenv
from flask import Flask, render_template, request, jsonify
from sqlalchemy import create_engine, func
from sqlalchemy.orm import Session

from save_indian_feeds import start_background_scheduler, init_db, INDIAN_OUTLETS
# FIX: import shared models instead of redefining RSSArticle and FetchLog here
from models import RSSArticle, FetchLog

load_dotenv()

# FIX: use DB_HOST env var (was hardcoded as 127.0.0.1 — inconsistent with fetch_full_articles.py)
DATABASE_URL = (
    f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST', 'localhost')}:{os.getenv('DB_PORT', '5432')}/{os.getenv('DB_NAME')}"
)
engine = create_engine(DATABASE_URL, echo=False)
app    = Flask(__name__)

@app.context_processor
def inject_now():
    return {"now": datetime.utcnow().strftime("%A, %d %B %Y")}

def get_stats():
    with Session(engine) as s:
        total     = s.query(RSSArticle).count()
        with_body = s.query(RSSArticle).filter(RSSArticle.body.isnot(None)).count()
        per_outlet= s.query(RSSArticle.outlet, RSSArticle.bias, func.count(RSSArticle.id)).group_by(RSSArticle.outlet, RSSArticle.bias).all()
        per_type  = s.query(RSSArticle.content_type, func.count(RSSArticle.id)).filter(RSSArticle.content_type.isnot(None)).group_by(RSSArticle.content_type).all()
        per_bias  = s.query(RSSArticle.bias, func.count(RSSArticle.id)).group_by(RSSArticle.bias).all()
        last_run  = s.query(FetchLog).order_by(FetchLog.run_at.desc()).first()
        return {
            "total": total, "with_body": with_body, "without_body": total - with_body,
            "per_outlet": [{"outlet": r[0], "bias": r[1], "count": r[2]} for r in per_outlet],
            "per_type":   [{"type": r[0] or "unknown", "count": r[1]} for r in per_type],
            "per_bias":   {r[0]: r[1] for r in per_bias},
            "last_run":   last_run.run_at.strftime("%Y-%m-%d %H:%M UTC") if last_run else "Never",
            "outlet_count": len(INDIAN_OUTLETS),
        }

def get_articles(outlet=None, bias=None, content_type=None, has_body=None, search=None, page=1, per_page=20):
    with Session(engine) as s:
        q = s.query(RSSArticle)
        if outlet:            q = q.filter(RSSArticle.outlet == outlet)
        if bias:              q = q.filter(RSSArticle.bias == bias)
        if content_type:      q = q.filter(RSSArticle.content_type == content_type)
        if has_body == "yes": q = q.filter(RSSArticle.body.isnot(None))
        if has_body == "no":  q = q.filter(RSSArticle.body.is_(None))
        if search:            q = q.filter(RSSArticle.title.ilike(f"%{search}%"))
        total   = q.count()
        results = q.order_by(RSSArticle.fetched_at.desc()).offset((page-1)*per_page).limit(per_page).all()
        return {
            "articles": [{"id": a.id, "outlet": a.outlet, "bias": a.bias, "title": a.title,
                          "url": a.url, "summary": a.summary,
                          "body_preview": a.body[:300]+"..." if a.body else None,
                          "body_length": len(a.body) if a.body else 0,
                          "content_type": a.content_type or "unknown",
                          "published": a.published, "has_body": a.body is not None} for a in results],
            "total": total, "page": page, "per_page": per_page,
            "pages": (total + per_page - 1) // per_page,
        }

def get_article_detail(article_id):
    with Session(engine) as s:
        a = s.query(RSSArticle).filter_by(id=article_id).first()
        if not a: return None
        return {"id": a.id, "outlet": a.outlet, "bias": a.bias, "title": a.title,
                "url": a.url, "summary": a.summary, "body": a.body,
                "body_length": len(a.body) if a.body else 0,
                "content_type": a.content_type or "unknown",
                "published": a.published, "has_body": a.body is not None}

def get_fetch_logs(page=1, per_page=50):
    with Session(engine) as s:
        total   = s.query(FetchLog).count()
        results = s.query(FetchLog).order_by(FetchLog.run_at.desc()).offset((page-1)*per_page).limit(per_page).all()
        return {
            "logs": [{"id": l.id, "run_id": l.run_id, "outlet": l.outlet,
                      "run_at": l.run_at.strftime("%Y-%m-%d %H:%M:%S") if l.run_at else "",
                      "articles_new": l.articles_new, "articles_skip": l.articles_skip,
                      "status": l.status, "error_message": l.error_message} for l in results],
            "total": total, "page": page, "pages": (total+per_page-1)//per_page,
        }

@app.route("/")
def index():
    return render_template("index.html", stats=get_stats())

@app.route("/articles")
def articles():
    data = get_articles(outlet=request.args.get("outlet"), bias=request.args.get("bias"),
                        content_type=request.args.get("content_type"), has_body=request.args.get("has_body"),
                        search=request.args.get("search"), page=int(request.args.get("page", 1)))
    return render_template("articles.html", data=data, outlets=list(INDIAN_OUTLETS.keys()),
                           filters={"outlet": request.args.get("outlet"), "bias": request.args.get("bias"),
                                    "content_type": request.args.get("content_type"),
                                    "has_body": request.args.get("has_body"), "search": request.args.get("search")})

@app.route("/article/<article_id>")
def article_detail(article_id):
    article = get_article_detail(article_id)
    if not article: return "Article not found", 404
    return render_template("article.html", article=article)

@app.route("/logs")
def fetch_logs():
    return render_template("logs.html", data=get_fetch_logs(page=int(request.args.get("page", 1))))

@app.route("/api/stats")
def api_stats():
    return jsonify(get_stats())

@app.route("/api/trigger-fetch")
def trigger_fetch():
    import threading
    from save_indian_feeds import run_collection
    threading.Thread(target=run_collection, daemon=True).start()
    return jsonify({"status": "started", "message": "Fetch triggered in background."})

if __name__ == "__main__":
    init_db()
    if os.environ.get("WERKZEUG_RUN_MAIN") != "true":
        start_background_scheduler(interval_hours=4)
    app.run(debug=True, port=5000, use_reloader=True)