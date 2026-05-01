import { useState, useEffect } from "react";
import { Link } from "react-router-dom";
import { API_BASE } from "../config";
import { BiasBar, LoadingState, EmptyState, timeAgo } from "./Shared";
import { Newspaper, TrendingUp, AlertTriangle, ArrowRight } from "lucide-react";

interface Story {
  id: string;
  title: string;
  summary: string | null;
  article_count: number;
  bias_distribution: Record<string, number> | null;
  disagreement_score: number | null;
  confidence_score: number | null;
  updated_at: string | null;
  created_at: string | null;
}

export default function HomePage() {
  const [stories, setStories] = useState<Story[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch(`${API_BASE}/stories`)
      .then((r) => r.json())
      .then((data) => {
        setStories(Array.isArray(data) ? data : []);
        setLoading(false);
      })
      .catch(() => setLoading(false));
  }, []);

  if (loading) return <LoadingState message="Loading stories..." />;

  // Stats
  const totalArticles = stories.reduce((s, st) => s + (st.article_count || 0), 0);
  const highDisagreement = stories.filter((s) => (s.disagreement_score || 0) > 0.6).length;

  return (
    <div className="container mx-auto px-4 py-8">
      <div className="space-y-8">
        {/* Header */}
        <div>
          <h1 className="text-3xl font-bold tracking-tight">Global Stories</h1>
          <p className="mt-1 text-sm" style={{ color: "var(--muted-foreground)" }}>
            Current narratives from multiple perspectives and sources
          </p>
        </div>

        {/* Stats cards */}
        <div className="grid gap-4 grid-cols-1 sm:grid-cols-3">
          <div className="card">
            <div className="card-content flex items-center gap-3 py-4">
              <div
                className="w-10 h-10 rounded-lg flex items-center justify-center"
                style={{ background: "var(--secondary)" }}
              >
                <Newspaper size={20} style={{ color: "var(--muted-foreground)" }} />
              </div>
              <div>
                <div className="text-2xl font-bold">{totalArticles}</div>
                <div className="text-xs" style={{ color: "var(--muted-foreground)" }}>
                  Total Articles
                </div>
              </div>
            </div>
          </div>

          <div className="card">
            <div className="card-content flex items-center gap-3 py-4">
              <div
                className="w-10 h-10 rounded-lg flex items-center justify-center"
                style={{ background: "var(--secondary)" }}
              >
                <TrendingUp size={20} style={{ color: "var(--muted-foreground)" }} />
              </div>
              <div>
                <div className="text-2xl font-bold">{stories.length}</div>
                <div className="text-xs" style={{ color: "var(--muted-foreground)" }}>
                  Active Stories
                </div>
              </div>
            </div>
          </div>

          <div className="card">
            <div className="card-content flex items-center gap-3 py-4">
              <div
                className="w-10 h-10 rounded-lg flex items-center justify-center"
                style={{ background: highDisagreement > 0 ? "#fef2f2" : "var(--secondary)" }}
              >
                <AlertTriangle
                  size={20}
                  style={{ color: highDisagreement > 0 ? "#dc2626" : "var(--muted-foreground)" }}
                />
              </div>
              <div>
                <div className="text-2xl font-bold">{highDisagreement}</div>
                <div className="text-xs" style={{ color: "var(--muted-foreground)" }}>
                  High Disagreement
                </div>
              </div>
            </div>
          </div>
        </div>

        {/* Story grid */}
        {stories.length === 0 ? (
          <EmptyState
            icon={<Newspaper size={48} />}
            title="No stories available yet"
            subtitle="Run the pipeline to fetch and cluster articles into stories"
          />
        ) : (
          <div className="grid gap-5 md:grid-cols-2 lg:grid-cols-3">
            {stories.map((story) => (
              <StoryCard key={story.id} story={story} />
            ))}
          </div>
        )}
      </div>
    </div>
  );
}

function StoryCard({ story }: { story: any }) {
  const disagreement = Math.round((story.disagreement_score || 0) * 100);

  return (
    <Link to={`/Story/${story.id}`} className="block no-underline">
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
            <div className="flex items-center gap-1.5">
              <span style={{ color: "var(--muted-foreground)" }}>Articles:</span>
              <span className="badge badge-secondary">{story.article_count}</span>
            </div>
            <div className="flex items-center gap-1.5">
              <span style={{ color: "var(--muted-foreground)" }}>Disagreement:</span>
              <span
                className={`badge ${disagreement > 60 ? "badge-destructive" : "badge-secondary"}`}
              >
                {disagreement}%
              </span>
            </div>
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
