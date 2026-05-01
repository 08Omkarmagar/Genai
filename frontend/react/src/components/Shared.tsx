import { formatDistanceToNow } from "date-fns";

export const biasColors: Record<string, string> = {
  left: "badge-left",
  "center-left": "badge-left",
  center: "badge-center",
  "center-right": "badge-right",
  right: "badge-right",
};

export function BiasTag({ bias }: { bias: string }) {
  const cls = biasColors[bias?.toLowerCase()] ?? "badge-secondary";
  return (
    <span className={`badge ${cls}`}>
      {bias}
    </span>
  );
}

export function formatDate(dateStr: string | null, includeWeekday = false) {
  if (!dateStr) return null;
  return new Date(dateStr).toLocaleDateString("en-US", {
    ...(includeWeekday && { weekday: "long" }),
    month: "long",
    day: "numeric",
    year: "numeric",
  });
}

export function timeAgo(dateStr: string | null) {
  if (!dateStr) return "";
  try {
    return formatDistanceToNow(new Date(dateStr), { addSuffix: true });
  } catch {
    return "";
  }
}

export function ConfidenceBadge({ score }: { score: number | null | undefined }) {
  if (score === null || score === undefined) return null;
  const percentage = (score * 100).toFixed(0);
  const color =
    score > 0.7 ? "#16a34a" : score > 0.4 ? "#ea580c" : "var(--muted-foreground)";

  return (
    <span className="badge badge-outline" style={{ color, borderColor: color }}>
      {percentage}% confidence
    </span>
  );
}

export function BiasBar({ distribution }: { distribution: Record<string, number> | null }) {
  if (!distribution) return null;
  const total = Object.values(distribution).reduce((a, b) => a + b, 0);
  if (total === 0) return null;

  const getPercent = (key: string) => {
    const val = distribution[key] || 0;
    return Math.round((val / total) * 100);
  };

  const leftPct = getPercent("left") + getPercent("center-left");
  const centerPct = getPercent("center") + getPercent("unknown");
  const rightPct = getPercent("right") + getPercent("center-right");

  return (
    <div className="space-y-1.5">
      <div className="text-xs" style={{ color: "var(--muted-foreground)" }}>Bias Distribution</div>
      <div className="bias-bar">
        {leftPct > 0 && (
          <div
            style={{ width: `${leftPct}%`, background: "var(--bias-left)" }}
            title={`Left: ${leftPct}%`}
          />
        )}
        {centerPct > 0 && (
          <div
            style={{ width: `${centerPct}%`, background: "var(--bias-center)" }}
            title={`Center: ${centerPct}%`}
          />
        )}
        {rightPct > 0 && (
          <div
            style={{ width: `${rightPct}%`, background: "var(--bias-right)" }}
            title={`Right: ${rightPct}%`}
          />
        )}
      </div>
      <div className="flex justify-between text-xs" style={{ color: "var(--muted-foreground)" }}>
        <span>Left {leftPct}%</span>
        <span>Center {centerPct}%</span>
        <span>Right {rightPct}%</span>
      </div>
    </div>
  );
}

export function LoadingState({ message = "Loading..." }: { message?: string }) {
  return (
    <div className="container mx-auto px-4 py-8">
      <div className="flex items-center justify-center min-h-[400px]">
        <div className="flex flex-col items-center gap-3">
          <div className="spinner" style={{ color: "var(--muted-foreground)" }}>
            <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
              <path d="M21 12a9 9 0 1 1-6.219-8.56" />
            </svg>
          </div>
          <p className="text-sm" style={{ color: "var(--muted-foreground)" }}>{message}</p>
        </div>
      </div>
    </div>
  );
}

export function EmptyState({ icon, title, subtitle }: { icon?: React.ReactNode; title: string; subtitle?: string }) {
  return (
    <div className="text-center py-12" style={{ color: "var(--muted-foreground)" }}>
      {icon && <div className="mb-4 opacity-40 flex justify-center">{icon}</div>}
      <p className="font-medium">{title}</p>
      {subtitle && <p className="text-sm mt-1">{subtitle}</p>}
    </div>
  );
}
