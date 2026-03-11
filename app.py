import os
import threading
from datetime import datetime, timezone
from dotenv import load_dotenv
from flask import Flask, render_template, request, jsonify
from sqlalchemy import create_engine, Column, String, Text, DateTime, Integer, func
from sqlalchemy.orm import DeclarativeBase, Session

from save_indian_feeds import init_db, INDIAN_OUTLETS

load_dotenv()

DATABASE_URL = (
    f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@127.0.0.1:{os.getenv('DB_PORT', '5432')}/{os.getenv('DB_NAME')}"
)
engine = create_engine(DATABASE_URL, echo=False)
app = Flask(__name__)

# ── JOB STATUS TRACKER ────────────────────────────────────────────
jobs = {
    "rss":  {"running": False, "last_run": None, "last_result": None, "last_error": None},
    "body": {"running": False, "last_run": None, "last_result": None, "last_error": None},
}
jobs_lock = threading.Lock()


@app.context_processor
def inject_now():
    return {"now": datetime.utcnow().strftime("%A, %d %B %Y")}


class Base(DeclarativeBase):
    pass


class RSSArticle(Base):
    __tablename__ = "rss_articles"
    id = Column(String,  primary_key=True)
    outlet = Column(String)
    bias = Column(String)
    country = Column(String)
    title = Column(Text)
    url = Column(String)
    summary = Column(Text)
    published = Column(String)
    body = Column(Text)
    body_fetched = Column(String)
    content_type = Column(String)
    fetched_at = Column(DateTime)


class FetchLog(Base):
    __tablename__ = "fetch_logs"
    id = Column(String,  primary_key=True)
    run_id = Column(String)
    outlet = Column(String)
    run_at = Column(DateTime)
    articles_new = Column(Integer)
    articles_skip = Column(Integer)
    status = Column(String)
    error_message = Column(Text)


# ── DATA HELPERS ──────────────────────────────────────────────────
def get_stats():
    with Session(engine) as s:
        total = s.query(RSSArticle).count()
        with_body = s.query(RSSArticle).filter(
            RSSArticle.body.isnot(None)).count()
        pending = s.query(RSSArticle).filter(
            RSSArticle.body.is_(None),
        ).count()
        per_outlet = s.query(RSSArticle.outlet, RSSArticle.bias, func.count(
            RSSArticle.id)).group_by(RSSArticle.outlet, RSSArticle.bias).all()
        per_type = s.query(RSSArticle.content_type, func.count(RSSArticle.id)).filter(
            RSSArticle.content_type.isnot(None)).group_by(RSSArticle.content_type).all()
        per_bias = s.query(RSSArticle.bias, func.count(
            RSSArticle.id)).group_by(RSSArticle.bias).all()
        last_rss = s.query(FetchLog).order_by(FetchLog.run_at.desc()).first()
        return {
            "total":        total,
            "with_body":    with_body,
            "without_body": total - with_body,
            "pending_body": pending,
            "per_outlet":   [{"outlet": r[0], "bias": r[1], "count": r[2]} for r in per_outlet],
            "per_type":     [{"type": r[0] or "unknown", "count": r[1]} for r in per_type],
            "per_bias":     {r[0]: r[1] for r in per_bias},
            "last_rss_run": last_rss.run_at.strftime("%d %b %Y, %H:%M UTC") if last_rss else "Never",
            "outlet_count": len(INDIAN_OUTLETS),
        }


def get_articles(outlet=None, bias=None, content_type=None, has_body=None, search=None, page=1, per_page=20):
    with Session(engine) as s:
        q = s.query(RSSArticle)
        if outlet:
            q = q.filter(RSSArticle.outlet == outlet)
        if bias:
            q = q.filter(RSSArticle.bias == bias)
        if content_type:
            q = q.filter(RSSArticle.content_type == content_type)
        if has_body == "yes":
            q = q.filter(RSSArticle.body.isnot(None))
        if has_body == "no":
            q = q.filter(RSSArticle.body.is_(None))
        if search:
            q = q.filter(RSSArticle.title.ilike(f"%{search}%"))
        total = q.count()
        results = q.order_by(RSSArticle.fetched_at.desc()).offset(
            (page-1)*per_page).limit(per_page).all()
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
        if not a:
            return None
        return {"id": a.id, "outlet": a.outlet, "bias": a.bias, "title": a.title,
                "url": a.url, "summary": a.summary, "body": a.body,
                "body_length": len(a.body) if a.body else 0,
                "content_type": a.content_type or "unknown",
                "published": a.published, "has_body": a.body is not None}


def get_fetch_logs(page=1, per_page=50):
    with Session(engine) as s:
        total = s.query(FetchLog).count()
        results = s.query(FetchLog).order_by(FetchLog.run_at.desc()).offset(
            (page-1)*per_page).limit(per_page).all()
        return {
            "logs": [{"id": l.id, "run_id": l.run_id, "outlet": l.outlet,
                      "run_at": l.run_at.strftime("%Y-%m-%d %H:%M:%S") if l.run_at else "",
                      "articles_new": l.articles_new, "articles_skip": l.articles_skip,
                      "status": l.status, "error_message": l.error_message} for l in results],
            "total": total, "page": page, "pages": (total+per_page-1)//per_page,
        }


# ── JOB RUNNERS ───────────────────────────────────────────────────
def _run_rss_job():
    with jobs_lock:
        if jobs["rss"]["running"]:
            return
        jobs["rss"]["running"] = True
        jobs["rss"]["last_error"] = None
    try:
        from save_indian_feeds import run_collection
        new_count = run_collection()
        with jobs_lock:
            jobs["rss"]["last_result"] = f"+{new_count} new articles collected"
    except Exception as e:
        with jobs_lock:
            jobs["rss"]["last_error"] = str(e)
            jobs["rss"]["last_result"] = "Failed — check feeds.log"
    finally:
        with jobs_lock:
            jobs["rss"]["running"] = False
            jobs["rss"]["last_run"] = datetime.now(
                timezone.utc).strftime("%d %b %Y, %H:%M UTC")


def _run_body_job():
    with jobs_lock:
        if jobs["body"]["running"]:
            return
        jobs["body"]["running"] = True
        jobs["body"]["last_error"] = None
    try:
        from fetch_full_articles import fetch_full_articles
        fetch_full_articles(batch_size=300, max_workers=10)
        with Session(engine) as s:
            done = s.query(RSSArticle).filter(
                RSSArticle.body.isnot(None)).count()
        with jobs_lock:
            jobs["body"]["last_result"] = f"{done} articles now have full body text"
    except Exception as e:
        with jobs_lock:
            jobs["body"]["last_error"] = str(e)
            jobs["body"]["last_result"] = "Failed — check terminal logs"
    finally:
        with jobs_lock:
            jobs["body"]["running"] = False
            jobs["body"]["last_run"] = datetime.now(
                timezone.utc).strftime("%d %b %Y, %H:%M UTC")


# ── ROUTES ────────────────────────────────────────────────────────
@app.route("/")
def index():
    return render_template("index.html", stats=get_stats(), jobs=jobs)


@app.route("/articles")
def articles():
    data = get_articles(
        outlet=request.args.get("outlet"), bias=request.args.get("bias"),
        content_type=request.args.get("content_type"), has_body=request.args.get("has_body"),
        search=request.args.get("search"), page=int(request.args.get("page", 1)))
    return render_template("articles.html", data=data, outlets=list(INDIAN_OUTLETS.keys()),
                           filters={"outlet": request.args.get("outlet"), "bias": request.args.get("bias"),
                                    "content_type": request.args.get("content_type"),
                                    "has_body": request.args.get("has_body"), "search": request.args.get("search")})


@app.route("/article/<article_id>")
def article_detail(article_id):
    article = get_article_detail(article_id)
    if not article:
        return "Article not found", 404
    return render_template("article.html", article=article)


@app.route("/logs")
def fetch_logs_page():
    return render_template("logs.html", data=get_fetch_logs(page=int(request.args.get("page", 1))))


# ── API ───────────────────────────────────────────────────────────
@app.route("/api/job-status")
def api_job_status():
    """Polled every 3s by the dashboard to update button states."""
    with jobs_lock:
        return jsonify({"rss": dict(jobs["rss"]), "body": dict(jobs["body"])})


@app.route("/api/trigger-rss", methods=["POST"])
def api_trigger_rss():
    with jobs_lock:
        if jobs["rss"]["running"]:
            return jsonify({"status": "already_running"})
    threading.Thread(target=_run_rss_job, daemon=True,
                     name="ManualRSSFetch").start()
    return jsonify({"status": "started"})


@app.route("/api/trigger-body", methods=["POST"])
def api_trigger_body():
    with jobs_lock:
        if jobs["body"]["running"]:
            return jsonify({"status": "already_running"})
    threading.Thread(target=_run_body_job, daemon=True,
                     name="ManualBodyFetch").start()
    return jsonify({"status": "started"})


@app.route("/api/stats")
def api_stats():
    return jsonify(get_stats())


# ── STARTUP ───────────────────────────────────────────────────────
if __name__ == "__main__":
    init_db()
    # if os.environ.get("WERKZEUG_RUN_MAIN") != "true":
    #     start_background_scheduler(interval_hours=4)
    if os.environ.get("WERKZEUG_RUN_MAIN") != "true":
        import threading
        import time

        def _scheduler():
            while True:
                try:
                    from save_indian_feeds import run_collection
                    run_collection()
                except Exception as e:
                    print(f"[Scheduler] Error: {e}")
                time.sleep(4 * 3600)
        threading.Thread(target=_scheduler, daemon=True,
                         name="FeedScheduler").start()
    app.run(debug=True, port=5000, use_reloader=True)
