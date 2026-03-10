import os
from datetime import datetime
from dotenv import load_dotenv
from flask import Flask, render_template, request, jsonify
from sqlalchemy import create_engine, Column, String, Text, DateTime, Boolean, func
from sqlalchemy.orm import DeclarativeBase, Session

load_dotenv()

DATABASE_URL = (
    f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@127.0.0.1:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)

engine = create_engine(DATABASE_URL, echo=False)

app = Flask(__name__)


@app.context_processor
def inject_now():
    return {"now": datetime.utcnow().strftime("%A, %d %B %Y")}


# ── MODEL ─────────────────────────────────────────────────────────
class Base(DeclarativeBase):
    pass


class RSSArticle(Base):
    __tablename__ = "rss_articles"

    id           = Column(String,  primary_key=True)
    outlet       = Column(String)
    bias         = Column(String)
    country      = Column(String)
    title        = Column(Text)
    url          = Column(String)
    summary      = Column(Text)
    published    = Column(String)
    body         = Column(Text)
    body_fetched = Column(Boolean)
    content_type = Column(String)
    fetched_at   = Column(DateTime)


# ── HELPERS ───────────────────────────────────────────────────────
def get_stats():
    with Session(engine) as s:
        total        = s.query(RSSArticle).count()
        with_body    = s.query(RSSArticle).filter(RSSArticle.body.isnot(None)).count()
        without_body = total - with_body

        per_outlet = (
            s.query(RSSArticle.outlet, RSSArticle.bias, func.count(RSSArticle.id))
            .group_by(RSSArticle.outlet, RSSArticle.bias)
            .all()
        )

        per_type = (
            s.query(RSSArticle.content_type, func.count(RSSArticle.id))
            .filter(RSSArticle.content_type.isnot(None))
            .group_by(RSSArticle.content_type)
            .all()
        )

        per_bias = (
            s.query(RSSArticle.bias, func.count(RSSArticle.id))
            .group_by(RSSArticle.bias)
            .all()
        )

        return {
            "total":        total,
            "with_body":    with_body,
            "without_body": without_body,
            "per_outlet":   [{"outlet": r[0], "bias": r[1], "count": r[2]} for r in per_outlet],
            "per_type":     [{"type": r[0] or "unknown", "count": r[1]} for r in per_type],
            "per_bias":     {r[0]: r[1] for r in per_bias},
        }


def get_articles(outlet=None, bias=None, content_type=None,
                 has_body=None, search=None, page=1, per_page=20):
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

        total   = q.count()
        results = q.order_by(RSSArticle.fetched_at.desc())\
                   .offset((page - 1) * per_page)\
                   .limit(per_page)\
                   .all()

        articles = []
        for a in results:
            articles.append({
                "id":           a.id,
                "outlet":       a.outlet,
                "bias":         a.bias,
                "title":        a.title,
                "url":          a.url,
                "summary":      a.summary,
                "body_preview": a.body[:300] + "..." if a.body else None,
                "body_length":  len(a.body) if a.body else 0,
                "content_type": a.content_type or "unknown",
                "published":    a.published,
                "has_body":     a.body is not None,
            })

        return {
            "articles":   articles,
            "total":      total,
            "page":       page,
            "per_page":   per_page,
            "pages":      (total + per_page - 1) // per_page,
        }


def get_article_detail(article_id):
    with Session(engine) as s:
        a = s.query(RSSArticle).filter_by(id=article_id).first()
        if not a:
            return None
        return {
            "id":           a.id,
            "outlet":       a.outlet,
            "bias":         a.bias,
            "title":        a.title,
            "url":          a.url,
            "summary":      a.summary,
            "body":         a.body,
            "body_length":  len(a.body) if a.body else 0,
            "content_type": a.content_type or "unknown",
            "published":    a.published,
            "has_body":     a.body is not None,
        }


# ── ROUTES ────────────────────────────────────────────────────────
@app.route("/")
def index():
    stats = get_stats()
    return render_template("index.html", stats=stats)


@app.route("/articles")
def articles():
    outlet       = request.args.get("outlet")
    bias         = request.args.get("bias")
    content_type = request.args.get("content_type")
    has_body     = request.args.get("has_body")
    search       = request.args.get("search")
    page         = int(request.args.get("page", 1))

    data    = get_articles(outlet, bias, content_type, has_body, search, page)
    outlets = ["The Hindu", "Times of India", "The Quint", "BBC India"]

    return render_template("articles.html",
                           data=data,
                           outlets=outlets,
                           filters={
                               "outlet":       outlet,
                               "bias":         bias,
                               "content_type": content_type,
                               "has_body":     has_body,
                               "search":       search,
                           })


@app.route("/article/<article_id>")
def article_detail(article_id):
    article = get_article_detail(article_id)
    if not article:
        return "Article not found", 404
    return render_template("article.html", article=article)


@app.route("/api/stats")
def api_stats():
    return jsonify(get_stats())


if __name__ == "__main__":
    app.run(debug=True, port=5000)
