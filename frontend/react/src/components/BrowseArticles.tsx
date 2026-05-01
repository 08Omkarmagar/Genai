import { useEffect, useState } from "react";
import { useParams, Link } from "react-router-dom";
import type { Article } from "./types";
import { API_BASE } from "../config";
import { BiasTag, formatDate, LoadingState } from "./Shared";
import { ArrowLeft, ExternalLink } from "lucide-react";

function parseParagraphs(body: string): string[] {
  return body
    .split("\n")
    .map((p) => p.trim())
    .filter(Boolean);
}

export default function BrowseArticles() {
  const { id } = useParams();
  const [article, setArticle] = useState<Article | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!id) return;
    setLoading(true);
    fetch(`${API_BASE}/articles/${id}`)
      .then((res) => {
        if (!res.ok) throw new Error("Article not found");
        return res.json();
      })
      .then(setArticle)
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }, [id]);

  if (loading) return <LoadingState message="Loading article..." />;

  if (error)
    return (
      <div className="container mx-auto px-4 py-8 max-w-4xl">
        <div className="text-center py-12">
          <p className="text-sm mb-4" style={{ color: "#dc2626" }}>{error}</p>
          <Link to="/Articles" className="btn btn-outline">
            <ArrowLeft size={14} />
            Back to Articles
          </Link>
        </div>
      </div>
    );

  if (!article) return null;

  const paragraphs = article.body
    ? parseParagraphs(article.body)
    : article.summary
      ? [article.summary]
      : [];

  const [firstPara, ...restParas] = paragraphs;

  return (
    <div className="container mx-auto px-4 py-8 max-w-4xl">
      {/* Back button */}
      <Link
        to="/Articles"
        className="inline-flex items-center gap-1.5 text-sm mb-8 no-underline transition-colors"
        style={{ color: "var(--muted-foreground)" }}
        onMouseEnter={(e) => (e.currentTarget.style.color = "var(--foreground)")}
        onMouseLeave={(e) => (e.currentTarget.style.color = "var(--muted-foreground)")}
      >
        <ArrowLeft size={16} />
        Back to Articles
      </Link>

      <article className="space-y-6">
        {/* Article header */}
        <div>
          <h1 className="text-3xl md:text-4xl font-bold leading-tight mb-4 tracking-tight">
            {article.title}
          </h1>
          <div className="flex flex-wrap items-center gap-3 text-sm" style={{ color: "var(--muted-foreground)" }}>
            <BiasTag bias={article.bias} />
            <span className="font-medium" style={{ color: "var(--foreground)" }}>
              {article.outlet}
            </span>
            {article.country && (
              <>
                <span>•</span>
                <span>{article.country}</span>
              </>
            )}
            <span>•</span>
            <span>{formatDate(article.published ?? article.fetched_at, true)}</span>
          </div>
        </div>

        {/* Summary callout */}
        {article.summary && article.body && (
          <div
            className="border-l-4 pl-4 py-3"
            style={{ borderColor: "var(--primary)" }}
          >
            <p
              className="text-base italic leading-relaxed"
              style={{ color: "var(--muted-foreground)", fontFamily: "var(--font-serif)" }}
            >
              {article.summary}
            </p>
          </div>
        )}

        {/* AI Summary */}
        {article.ai_summary && (
          <div className="card" style={{ background: "var(--secondary)" }}>
            <div className="card-header">
              <h3 className="card-title text-sm font-semibold">AI Summary</h3>
            </div>
            <div className="card-content">
              <p className="text-sm leading-relaxed" style={{ color: "var(--muted-foreground)" }}>
                {article.ai_summary}
              </p>
            </div>
          </div>
        )}

        {/* Article body */}
        {paragraphs.length > 0 ? (
          <div className="card">
            <div className="card-content">
              <div className="prose-body">
                {firstPara && (
                  <p className="mb-5">
                    <span
                      className="float-left text-5xl font-bold leading-none mr-2 mt-1"
                      style={{ fontFamily: "var(--font-serif)", lineHeight: "0.85" }}
                    >
                      {firstPara[0]}
                    </span>
                    {firstPara.slice(1)}
                  </p>
                )}
                {restParas.map((para, i) => (
                  <p key={i} className="mb-5">
                    {para}
                  </p>
                ))}
              </div>
            </div>
          </div>
        ) : (
          <div className="card">
            <div className="card-content py-10 text-center" style={{ color: "var(--muted-foreground)" }}>
              <p className="text-sm">Full article body not yet fetched</p>
              {article.url && (
                <a
                  href={article.url}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="btn btn-outline mt-3 inline-flex"
                >
                  <ExternalLink size={14} />
                  Read on original site
                </a>
              )}
            </div>
          </div>
        )}

        {/* Bias analysis */}
        {article.bias_reasoning && (
          <div className="card">
            <div className="card-header">
              <h3 className="card-title text-sm font-semibold">Bias Analysis</h3>
            </div>
            <div className="card-content space-y-2">
              <p className="text-sm leading-relaxed" style={{ color: "var(--muted-foreground)" }}>
                {article.bias_reasoning}
              </p>
              {article.bias_score !== null && article.bias_score !== undefined && (
                <div className="flex items-center gap-2">
                  <span className="text-xs font-medium" style={{ color: "var(--muted-foreground)" }}>
                    Bias Score:
                  </span>
                  <span
                    className={`badge ${(article.bias_score || 0) > 5 ? "badge-destructive" : "badge-secondary"}`}
                  >
                    {article.bias_score}/10
                  </span>
                </div>
              )}
            </div>
          </div>
        )}

        {/* External link */}
        {article.url && (
          <div className="flex gap-3">
            <a
              href={article.url}
              target="_blank"
              rel="noopener noreferrer"
              className="btn btn-outline"
            >
              <ExternalLink size={14} />
              View Original Source
            </a>
          </div>
        )}

        {/* End marker */}
        <div className="flex items-center gap-4 pt-4">
          <div className="flex-1 border-t" />
          <span className="text-xs" style={{ color: "var(--muted-foreground)" }}>
            End of Article
          </span>
          <div className="flex-1 border-t" />
        </div>
      </article>
    </div>
  );
}
