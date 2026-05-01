import { useEffect, useState } from "react";
import { useSearchParams, Link } from "react-router-dom";
import { API_BASE } from "../config";
import { BiasBar, LoadingState, EmptyState, timeAgo } from "./Shared";
import { Search as SearchIcon, ArrowRight } from "lucide-react";

interface Story {
  id: string;
  title: string;
  summary: string | null;
  article_count: number;
  bias_distribution: Record<string, number> | null;
  disagreement_score: number | null;
  updated_at: string | null;
}

export default function SearchPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const [query, setQuery] = useState(searchParams.get("q") || "");
  const [results, setResults] = useState<Story[]>([]);
  const [loading, setLoading] = useState(false);
  const [searched, setSearched] = useState(false);

  useEffect(() => {
    const q = searchParams.get("q");
    if (q) {
      setQuery(q);
      performSearch(q);
    }
  }, []);

  const performSearch = async (searchQuery: string) => {
    if (!searchQuery.trim()) return;
    setLoading(true);
    setSearched(true);
    try {
      const res = await fetch(`${API_BASE}/search/stories?q=${encodeURIComponent(searchQuery)}`);
      if (!res.ok) throw new Error("Search failed");
      const data = await res.json();
      setResults(Array.isArray(data) ? data : []);
    } catch {
      setResults([]);
    } finally {
      setLoading(false);
    }
  };

  const handleSearch = (e: React.FormEvent) => {
    e.preventDefault();
    if (query.trim()) {
      setSearchParams({ q: query });
      performSearch(query);
    }
  };

  return (
    <div className="container mx-auto px-4 py-8">
      <div className="space-y-6">
        <div>
          <h1 className="text-3xl font-bold tracking-tight">Search Stories</h1>
          <p className="mt-1 text-sm" style={{ color: "var(--muted-foreground)" }}>
            Find stories using keyword and semantic search
          </p>
        </div>

        {/* Search form */}
        <form onSubmit={handleSearch} className="flex gap-2">
          <div className="flex-1 relative">
            <SearchIcon
              size={15}
              className="absolute left-3 top-1/2 -translate-y-1/2"
              style={{ color: "var(--muted-foreground)" }}
            />
            <input
              type="search"
              placeholder="Search for stories (e.g., 'climate', 'AI regulation', 'economic policy')"
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              className="input input-search w-full"
              style={{ paddingLeft: "2.5rem" }}
            />
          </div>
          <button type="submit" disabled={!query.trim() || loading} className="btn btn-primary">
            {loading ? "Searching…" : "Search"}
          </button>
        </form>

        {/* Results */}
        {loading && <LoadingState message="Searching stories..." />}

        {!loading && searched && (
          <>
            {results.length > 0 ? (
              <>
                <div className="text-sm" style={{ color: "var(--muted-foreground)" }}>
                  Found {results.length} {results.length === 1 ? "story" : "stories"} for "{searchParams.get("q")}"
                </div>
                <div className="grid gap-5 md:grid-cols-2 lg:grid-cols-3">
                  {results.map((story) => (
                    <SearchStoryCard key={story.id} story={story} />
                  ))}
                </div>
              </>
            ) : (
              <EmptyState
                icon={<SearchIcon size={48} />}
                title={`No stories found for "${searchParams.get("q")}"`}
                subtitle="Try different keywords or broader terms"
              />
            )}
          </>
        )}

        {!searched && (
          <EmptyState
            icon={<SearchIcon size={48} />}
            title="Enter a search query to find stories"
            subtitle="Search uses hybrid matching: keyword validation followed by semantic fallback"
          />
        )}
      </div>
    </div>
  );
}

function SearchStoryCard({ story }: { story: any }) {
  const disagreement = Math.round((story.disagreement_score || 0) * 100);

  return (
    <Link to={`/BrowseArticles/${story.id}`} className="block no-underline">
      <div className="card group" style={{ cursor: "pointer" }}>
        <div className="card-header">
          <h3 className="card-title group-hover:underline decoration-1 underline-offset-2">
            {story.title}
          </h3>
        </div>
        <div className="card-content space-y-3">
          {story.summary && (
            <p className="text-sm line-clamp-2" style={{ color: "var(--muted-foreground)" }}>
              {story.summary}
            </p>
          )}
          <div className="flex items-center gap-3 text-sm">
            <span className="badge badge-secondary">{story.article_count} articles</span>
            <span className={`badge ${disagreement > 60 ? "badge-destructive" : "badge-secondary"}`}>
              {disagreement}% disagreement
            </span>
          </div>
          <BiasBar distribution={story.bias_distribution} />
          <div
            className="flex items-center justify-between text-xs pt-2 border-t"
            style={{ color: "var(--muted-foreground)" }}
          >
            <span>{timeAgo(story.updated_at)}</span>
            <ArrowRight size={14} />
          </div>
        </div>
      </div>
    </Link>
  );
}
