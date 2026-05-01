import { useState, useEffect } from "react";
import { API_BASE } from "../config";
import { BiasBar, LoadingState, EmptyState } from "./Shared";
import { BarChart3 } from "lucide-react";

interface Story {
  id: string;
  title: string;
  summary: string | null;
  article_count: number;
  bias_distribution: Record<string, number> | null;
  disagreement_score: number | null;
}

export default function InsightsPage() {
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

  if (loading) return <LoadingState message="Loading insights..." />;

  if (stories.length === 0) {
    return (
      <div className="container mx-auto px-4 py-8">
        <EmptyState
          icon={<BarChart3 size={48} />}
          title="No insights yet"
          subtitle="Run the pipeline to generate stories and bias analysis"
        />
      </div>
    );
  }

  // Aggregate bias
  const overallBias = stories.reduce(
    (acc, story) => {
      const dist = story.bias_distribution || {};
      return {
        left: acc.left + (dist.left || 0) + (dist["center-left"] || 0),
        center: acc.center + (dist.center || 0) + (dist.unknown || 0),
        right: acc.right + (dist.right || 0) + (dist["center-right"] || 0),
      };
    },
    { left: 0, center: 0, right: 0 }
  );

  const total = overallBias.left + overallBias.center + overallBias.right;
  const normalized = total > 0 ? {
    left: Math.round((overallBias.left / total) * 100),
    center: Math.round((overallBias.center / total) * 100),
    right: Math.round((overallBias.right / total) * 100),
  } : { left: 33, center: 34, right: 33 };

  // Sort stories by disagreement
  const sortedByDisagreement = [...stories].sort(
    (a, b) => (b.disagreement_score || 0) - (a.disagreement_score || 0)
  );

  return (
    <div className="container mx-auto px-4 py-8 max-w-6xl">
      <div className="space-y-8">
        {/* Header */}
        <div>
          <h1 className="text-3xl font-bold tracking-tight">Insights & Bias Analysis</h1>
          <p className="mt-1 text-sm" style={{ color: "var(--muted-foreground)" }}>
            Visual understanding of bias distribution and disagreement patterns
          </p>
        </div>

        {/* Overall bias */}
        <div className="card">
          <div className="card-header">
            <h3 className="card-title">Overall Bias Distribution</h3>
            <p className="text-sm mt-0.5" style={{ color: "var(--muted-foreground)" }}>
              Aggregate bias across all {stories.length} current stories
            </p>
          </div>
          <div className="card-content">
            <div className="max-w-md">
              <div className="space-y-3">
                <BiasBarLarge label="Left" value={normalized.left} color="var(--bias-left)" />
                <BiasBarLarge label="Center" value={normalized.center} color="var(--bias-center)" />
                <BiasBarLarge label="Right" value={normalized.right} color="var(--bias-right)" />
              </div>
            </div>
          </div>
        </div>

        {/* Disagreement rankings */}
        <div className="card">
          <div className="card-header">
            <h3 className="card-title">Disagreement Scores by Story</h3>
            <p className="text-sm mt-0.5" style={{ color: "var(--muted-foreground)" }}>
              Higher scores indicate greater disagreement between sources
            </p>
          </div>
          <div className="card-content space-y-3">
            {sortedByDisagreement.map((story) => {
              const pct = Math.round((story.disagreement_score || 0) * 100);
              return (
                <div key={story.id} className="flex items-center gap-3">
                  <div
                    className="flex-1 min-w-0 text-sm font-medium truncate"
                    title={story.title}
                  >
                    {story.title}
                  </div>
                  <div className="w-40 flex items-center gap-2 shrink-0">
                    <div
                      className="flex-1 h-2 rounded-full overflow-hidden"
                      style={{ background: "var(--muted)" }}
                    >
                      <div
                        className="h-full rounded-full transition-all"
                        style={{
                          width: `${pct}%`,
                          background: pct > 60 ? "#dc2626" : pct > 30 ? "#f59e0b" : "#16a34a",
                        }}
                      />
                    </div>
                    <span
                      className="text-xs font-bold tabular-nums w-8 text-right"
                      style={{
                        color: pct > 60 ? "#dc2626" : pct > 30 ? "#f59e0b" : "#16a34a",
                      }}
                    >
                      {pct}%
                    </span>
                  </div>
                </div>
              );
            })}
          </div>
        </div>

        {/* Per-story breakdown */}
        <div className="grid gap-5 md:grid-cols-2">
          {stories.map((story) => (
            <div key={story.id} className="card">
              <div className="card-header">
                <h3 className="card-title text-sm">{story.title}</h3>
              </div>
              <div className="card-content space-y-3">
                <BiasBar distribution={story.bias_distribution} />
                <div className="flex justify-between text-xs" style={{ color: "var(--muted-foreground)" }}>
                  <span>{story.article_count} articles</span>
                  <span>{Math.round((story.disagreement_score || 0) * 100)}% disagreement</span>
                </div>
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

function BiasBarLarge({ label, value, color }: { label: string; value: number; color: string }) {
  return (
    <div className="flex items-center gap-3">
      <span className="text-sm font-medium w-16">{label}</span>
      <div
        className="flex-1 h-6 rounded-md overflow-hidden"
        style={{ background: "var(--muted)" }}
      >
        <div
          className="h-full rounded-md transition-all duration-500 flex items-center px-2"
          style={{ width: `${Math.max(value, 2)}%`, background: color }}
        >
          {value > 10 && (
            <span className="text-xs font-bold text-white">{value}%</span>
          )}
        </div>
      </div>
      {value <= 10 && (
        <span className="text-xs font-bold tabular-nums w-8 text-right">{value}%</span>
      )}
    </div>
  );
}
