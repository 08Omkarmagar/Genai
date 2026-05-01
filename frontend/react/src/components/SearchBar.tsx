import { useEffect, useState, useRef } from "react";
import { API_BASE } from "../config";

export interface SearchFilters {
  q: string;
  bias: string;
  outlet: string;
  date_from: string;
  date_to: string;
}

export const defaultFilters: SearchFilters = {
  q: "",
  bias: "",
  outlet: "",
  date_from: "",
  date_to: "",
};

interface Props {
  onSearch: (filters: SearchFilters) => void;
  loading?: boolean;
}

const BIAS_OPTIONS = [
  "",
  "left",
  "center-left",
  "center",
  "center-right",
  "right",
] as const;

export default function SearchBar({ onSearch, loading }: Props) {
  const [filters, setFilters] = useState<SearchFilters>(defaultFilters);
  const [outlets, setOutlets] = useState<string[]>([]);
  const [expanded, setExpanded] = useState(false);
  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => {
    fetch(`${API_BASE}/outlets`)
      .then((r) => r.json())
      .then(setOutlets)
      .catch(() => {});
  }, []);

  const handleChange = (
    key: keyof SearchFilters,
    value: string,
    debounce = false,
  ) => {
    const next = { ...filters, [key]: value };
    setFilters(next);

    if (debounce) {
      // Only keyword search waits; filters should feel immediate.
      if (debounceRef.current) clearTimeout(debounceRef.current);
      debounceRef.current = setTimeout(() => onSearch(next), 500);
    } else {
      onSearch(next);
    }
  };

  const handleReset = () => {
    setFilters(defaultFilters);
    onSearch(defaultFilters);
  };

  const hasActiveFilters = Object.values(filters).some((v) => v !== "");
  const activeCount = Object.values(filters).filter((v) => v !== "").length;

  return (
    <div className="sticky top-0 z-30 bg-white border-b-2 border-black shadow-sm">
      <div className="max-w-5xl mx-auto px-6 py-3 flex items-center gap-3">
        <div className="flex-1 flex items-center gap-2 border border-gray-300 border-t-2 border-t-black px-3 py-2 focus-within:border-black transition-colors">
          <span className="text-gray-400 text-sm shrink-0">⌕</span>
          <input
            type="text"
            value={filters.q}
            onChange={(e) => handleChange("q", e.target.value, true)}
            placeholder="Search titles and summaries…"
            className="flex-1 text-sm bg-transparent outline-none placeholder-gray-300 text-gray-800"
            style={{ fontFamily: "'Georgia', serif" }}
          />
          {filters.q && (
            <button
              onClick={() => handleChange("q", "")}
              className="text-gray-300 hover:text-black transition-colors text-xs shrink-0"
            >
              ✕
            </button>
          )}
        </div>

        <button
          onClick={() => setExpanded((v) => !v)}
          className={`flex items-center gap-2 px-4 py-2 border-2 text-[11px] font-bold uppercase tracking-widest transition-all ${
            expanded || hasActiveFilters
              ? "border-black bg-black text-white"
              : "border-gray-300 text-gray-500 hover:border-black hover:text-black"
          }`}
        >
          <span>Filters</span>
          {activeCount > 0 && (
            <span
              className={`text-[9px] rounded-full w-4 h-4 flex items-center justify-center font-black ${
                expanded ? "bg-white text-black" : "bg-black text-white"
              }`}
            >
              {activeCount}
            </span>
          )}
          <span className="text-[10px]">{expanded ? "▴" : "▾"}</span>
        </button>

        {hasActiveFilters && (
          <button
            onClick={handleReset}
            className="text-[10px] uppercase tracking-widest text-gray-400 hover:text-black border-b border-gray-200 hover:border-black transition-colors whitespace-nowrap"
          >
            Clear all
          </button>
        )}

        {loading && (
          <span className="text-[10px] uppercase tracking-widest text-gray-300 animate-pulse whitespace-nowrap">
            Searching…
          </span>
        )}
      </div>

      {expanded && (
        <div className="max-w-5xl mx-auto px-6 pb-3 grid grid-cols-1 sm:grid-cols-3 gap-3 border-t border-gray-100 pt-3">
          <div className="flex flex-col gap-1">
            <label className="text-[9px] uppercase tracking-widest text-gray-400 font-bold">
              Bias
            </label>
            <div className="flex gap-1">
              {BIAS_OPTIONS.map((b) => (
                <button
                  key={b || "all"}
                  onClick={() => handleChange("bias", b)}
                  className={`flex-1 px-2 py-1.5 text-[10px] font-bold uppercase tracking-widest border transition-all ${
                    filters.bias === b
                      ? b === "left"
                        ? "bg-blue-600  text-white border-blue-600"
                        : b === "right"
                          ? "bg-red-600   text-white border-red-600"
                          : b === "center"
                            ? "bg-gray-700  text-white border-gray-700"
                            : "bg-black      text-white border-black"
                      : "border-gray-200 text-gray-400 hover:border-gray-400 hover:text-black"
                  }`}
                >
                  {b || "All"}
                </button>
              ))}
            </div>
          </div>

          <div className="flex flex-col gap-1">
            <label className="text-[9px] uppercase tracking-widest text-gray-400 font-bold">
              Outlet
            </label>
            <select
              value={filters.outlet}
              onChange={(e) => handleChange("outlet", e.target.value)}
              className="border border-gray-200 border-t-2 border-t-black px-3 py-1.5 text-[11px] text-gray-700 bg-white outline-none focus:border-black transition-colors appearance-none"
              style={{ fontFamily: "'Georgia', serif" }}
            >
              <option value="">All outlets</option>
              {outlets.map((o) => (
                <option key={o} value={o}>
                  {o}
                </option>
              ))}
            </select>
          </div>

          <div className="flex flex-col gap-1">
            <label className="text-[9px] uppercase tracking-widest text-gray-400 font-bold">
              Date Range
            </label>
            <div className="flex items-center gap-2">
              <input
                type="date"
                value={filters.date_from}
                onChange={(e) => handleChange("date_from", e.target.value)}
                className="flex-1 border border-gray-200 border-t-2 border-t-black px-2 py-1.5 text-[11px] text-gray-700 bg-white outline-none focus:border-black transition-colors"
              />
              <span className="text-[10px] text-gray-300 shrink-0">→</span>
              <input
                type="date"
                value={filters.date_to}
                onChange={(e) => handleChange("date_to", e.target.value)}
                className="flex-1 border border-gray-200 border-t-2 border-t-black px-2 py-1.5 text-[11px] text-gray-700 bg-white outline-none focus:border-black transition-colors"
              />
            </div>
          </div>
        </div>
      )}

      {!expanded && hasActiveFilters && (
        <div className="max-w-5xl mx-auto px-6 pb-2 flex flex-wrap gap-1.5">
          {filters.bias && (
            <FilterPill
              label={`Bias: ${filters.bias}`}
              onRemove={() => handleChange("bias", "")}
            />
          )}
          {filters.outlet && (
            <FilterPill
              label={`Outlet: ${filters.outlet}`}
              onRemove={() => handleChange("outlet", "")}
            />
          )}
          {filters.date_from && (
            <FilterPill
              label={`From: ${filters.date_from}`}
              onRemove={() => handleChange("date_from", "")}
            />
          )}
          {filters.date_to && (
            <FilterPill
              label={`To: ${filters.date_to}`}
              onRemove={() => handleChange("date_to", "")}
            />
          )}
        </div>
      )}
    </div>
  );
}

function FilterPill({
  label,
  onRemove,
}: {
  label: string;
  onRemove: () => void;
}) {
  return (
    <span className="flex items-center gap-1.5 text-[9px] uppercase tracking-widest bg-black text-white px-2 py-0.5">
      {label}
      <button
        onClick={onRemove}
        className="hover:text-gray-300 transition-colors"
      >
        ✕
      </button>
    </span>
  );
}
