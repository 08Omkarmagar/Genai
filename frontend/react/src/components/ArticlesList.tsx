import { useEffect, useState, useRef } from "react";
import { Link } from "react-router-dom";
import type { Article } from "./types";
import { PAGE_SIZE, API_BASE } from "../config";
import { BiasTag, timeAgo, ConfidenceBadge, LoadingState } from "./Shared";
import { Search, SlidersHorizontal, X, ChevronDown, FileText } from "lucide-react";

export interface SearchFilters {
  q: string;
  bias: string;
  outlet: string;
  date_from: string;
  date_to: string;
}

const defaultFilters: SearchFilters = {
  q: "",
  bias: "",
  outlet: "",
  date_from: "",
  date_to: "",
};

const BIAS_OPTIONS = ["", "left", "center-left", "center", "center-right", "right"] as const;

function buildSearchUrl(filters: SearchFilters, limit: number, offset: number): string {
  const params = new URLSearchParams();
  params.append("limit", limit.toString());
  params.append("offset", offset.toString());
  if (filters.q) params.append("q", filters.q);
  if (filters.bias) params.append("bias", filters.bias);
  if (filters.outlet) params.append("outlet", filters.outlet);
  if (filters.date_from) params.append("date_from", filters.date_from);
  if (filters.date_to) params.append("date_to", filters.date_to);
  return `${API_BASE}/articles?${params.toString()}`;
}

async function loadArticles(filters: SearchFilters, offset: number): Promise<Article[]> {
  const url = buildSearchUrl(filters, PAGE_SIZE, offset);
  const res = await fetch(url);
  if (!res.ok) throw new Error("Failed to load articles");
  return res.json();
}

export default function ArticleList() {
  const [filters, setFilters] = useState<SearchFilters>(defaultFilters);
  const [articles, setArticles] = useState<Article[]>([]);
  const [offset, setOffset] = useState(0);
  const [hasMore, setHasMore] = useState(true);
  const [pageLoading, setPageLoading] = useState(false);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [expanded, setExpanded] = useState(false);
  const [outlets, setOutlets] = useState<string[]>([]);
  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => {
    fetch(`${API_BASE}/outlets`)
      .then((r) => r.json())
      .then(setOutlets)
      .catch(() => {});
  }, []);

  useEffect(() => {
    loadArticles(defaultFilters, 0)
      .then((data) => {
        setArticles(data);
        setHasMore(data.length === PAGE_SIZE);
      })
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }, []);

  const applyFilters = (newFilters: SearchFilters) => {
    setFilters(newFilters);
    setPageLoading(true);
    setError(null);
    loadArticles(newFilters, 0)
      .then((data) => {
        setArticles(data);
        setOffset(0);
        setHasMore(data.length === PAGE_SIZE);
      })
      .catch((e) => setError(e.message))
      .finally(() => setPageLoading(false));
  };

  const handleChange = (key: keyof SearchFilters, value: string, debounce = false) => {
    const next = { ...filters, [key]: value };
    setFilters(next);
    if (debounce) {
      if (debounceRef.current) clearTimeout(debounceRef.current);
      debounceRef.current = setTimeout(() => applyFilters(next), 500);
    } else {
      applyFilters(next);
    }
  };

  const handleReset = () => {
    setFilters(defaultFilters);
    applyFilters(defaultFilters);
  };

  const handleLoadMore = async () => {
    const nextOffset = offset + PAGE_SIZE;
    setPageLoading(true);
    try {
      const data = await loadArticles(filters, nextOffset);
      setArticles((prev) => [...prev, ...data]);
      setOffset(nextOffset);
      setHasMore(data.length === PAGE_SIZE);
    } catch {
      setError("Failed to load more articles");
    } finally {
      setPageLoading(false);
    }
  };

  const hasActiveFilters = Object.values(filters).some((v) => v !== "");
  const activeCount = Object.values(filters).filter((v) => v !== "").length;

  if (loading) return <LoadingState message="Loading articles..." />;

  return (
    <div className="container mx-auto px-4 py-8">
      <div className="space-y-6">
        {/* Header */}
        <div>
          <h1 className="text-3xl font-bold tracking-tight">Articles</h1>
          <p className="mt-1 text-sm" style={{ color: "var(--muted-foreground)" }}>
            Browse raw news articles from global sources
          </p>
        </div>

        {/* Search & Filters */}
        <div className="card">
          <div className="card-content py-3">
            <div className="flex items-center gap-3">
              <div className="flex-1 relative">
                <Search
                  size={15}
                  className="absolute left-2.5 top-1/2 -translate-y-1/2"
                  style={{ color: "var(--muted-foreground)" }}
                />
                <input
                  type="text"
                  value={filters.q}
                  onChange={(e) => handleChange("q", e.target.value, true)}
                  placeholder="Search titles and summaries…"
                  className="input input-search w-full"
                  style={{ paddingLeft: "2.5rem" }}
                />
              </div>

              <button
                onClick={() => setExpanded(!expanded)}
                className="btn btn-outline flex items-center gap-2"
                style={{
                  background: expanded || hasActiveFilters ? "var(--primary)" : undefined,
                  color: expanded || hasActiveFilters ? "var(--primary-foreground)" : undefined,
                  borderColor: expanded || hasActiveFilters ? "var(--primary)" : undefined,
                }}
              >
                <SlidersHorizontal size={14} />
                <span className="text-sm">Filters</span>
                {activeCount > 0 && (
                  <span
                    className="text-xs rounded-full w-5 h-5 flex items-center justify-center font-bold"
                    style={{
                      background: expanded ? "var(--primary-foreground)" : "var(--primary)",
                      color: expanded ? "var(--primary)" : "var(--primary-foreground)",
                    }}
                  >
                    {activeCount}
                  </span>
                )}
              </button>

              {hasActiveFilters && (
                <button
                  onClick={handleReset}
                  className="btn btn-ghost text-sm"
                  style={{ color: "var(--muted-foreground)" }}
                >
                  <X size={14} />
                  Clear
                </button>
              )}
            </div>

            {expanded && (
              <div className="grid grid-cols-1 sm:grid-cols-3 gap-4 mt-4 pt-4 border-t">
                <div className="flex flex-col gap-1.5">
                  <label className="text-xs font-medium" style={{ color: "var(--muted-foreground)" }}>
                    Bias
                  </label>
                  <div className="flex flex-wrap gap-1">
                    {BIAS_OPTIONS.map((b) => (
                      <button
                        key={b || "all"}
                        onClick={() => handleChange("bias", b)}
                        className="btn text-xs py-1 px-2.5"
                        style={{
                          background: filters.bias === b
                            ? b === "left" ? "#3b82f6"
                            : b === "right" ? "#ef4444"
                            : b === "center" ? "#6b7280"
                            : "var(--primary)"
                            : "var(--secondary)",
                          color: filters.bias === b ? "#fff" : "var(--foreground)",
                        }}
                      >
                        {b || "All"}
                      </button>
                    ))}
                  </div>
                </div>

                <div className="flex flex-col gap-1.5">
                  <label className="text-xs font-medium" style={{ color: "var(--muted-foreground)" }}>
                    Outlet
                  </label>
                  <select
                    value={filters.outlet}
                    onChange={(e) => handleChange("outlet", e.target.value)}
                    className="select-trigger"
                  >
                    <option value="">All outlets</option>
                    {outlets.map((o) => (
                      <option key={o} value={o}>{o}</option>
                    ))}
                  </select>
                </div>

                <div className="flex flex-col gap-1.5">
                  <label className="text-xs font-medium" style={{ color: "var(--muted-foreground)" }}>
                    Date Range
                  </label>
                  <div className="flex items-center gap-2">
                    <input
                      type="date"
                      value={filters.date_from}
                      onChange={(e) => handleChange("date_from", e.target.value)}
                      className="input text-sm flex-1"
                    />
                    <span className="text-xs" style={{ color: "var(--muted-foreground)" }}>→</span>
                    <input
                      type="date"
                      value={filters.date_to}
                      onChange={(e) => handleChange("date_to", e.target.value)}
                      className="input text-sm flex-1"
                    />
                  </div>
                </div>
              </div>
            )}
          </div>
        </div>

        {/* Results */}
        {error && articles.length === 0 ? (
          <div className="text-center py-12">
            <p className="text-sm" style={{ color: "#dc2626" }}>{error}</p>
          </div>
        ) : articles.length === 0 ? (
          <div className="card">
            <div className="card-content py-12 text-center" style={{ color: "var(--muted-foreground)" }}>
              <FileText size={40} className="mx-auto mb-3 opacity-40" />
              <p className="font-medium">No articles match your filters</p>
              <p className="text-sm mt-1">Try adjusting your search criteria</p>
            </div>
          </div>
        ) : (
          <>
            <div className="text-sm" style={{ color: "var(--muted-foreground)" }}>
              {articles.length} article{articles.length !== 1 ? "s" : ""} found
            </div>

            <div className="grid gap-4">
              {articles.map((article) => (
                <ArticleCard key={article.id} article={article} />
              ))}
            </div>

            {/* Load more */}
            <div className="flex flex-col items-center gap-3 pt-6 border-t">
              {hasMore ? (
                <button
                  onClick={handleLoadMore}
                  disabled={pageLoading}
                  className="btn btn-outline px-8 py-2.5"
                >
                  {pageLoading ? (
                    <>
                      <span className="spinner inline-block">
                        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                          <path d="M21 12a9 9 0 1 1-6.219-8.56" />
                        </svg>
                      </span>
                      Loading…
                    </>
                  ) : (
                    <>
                      <ChevronDown size={14} />
                      Load More Articles
                    </>
                  )}
                </button>
              ) : (
                <p className="text-xs" style={{ color: "var(--muted-foreground)" }}>
                  You've reached the end
                </p>
              )}
              <p className="text-xs" style={{ color: "var(--muted-foreground)" }}>
                {articles.length} articles loaded
              </p>
            </div>
          </>
        )}
      </div>
    </div>
  );
}

function ArticleCard({ article }: { article: Article }) {
  return (
    <Link to={`/BrowseArticles/${article.id}`} className="block no-underline">
      <div className="card group" style={{ cursor: "pointer" }}>
        <div className="card-content py-4">
          <div className="flex items-start justify-between gap-3">
            <div className="flex-1 min-w-0">
              <h3 className="font-semibold group-hover:underline decoration-1 underline-offset-2 mb-1.5">
                {article.title}
              </h3>
              {article.summary && (
                <p className="text-sm line-clamp-2 mb-2" style={{ color: "var(--muted-foreground)" }}>
                  {article.summary}
                </p>
              )}
              <div className="flex flex-wrap items-center gap-2 text-xs" style={{ color: "var(--muted-foreground)" }}>
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
                <span>{timeAgo(article.published || article.fetched_at)}</span>
              </div>
            </div>
            <div className="flex flex-col items-end gap-1.5 shrink-0">
              <BiasTag bias={article.bias} />
              <ConfidenceBadge score={article.confidence_score} />
            </div>
          </div>
        </div>
      </div>
    </Link>
  );
}
