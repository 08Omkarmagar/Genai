
export interface Article {
  id: string;
  outlet: string;
  bias: string;
  country: string | null;
  title: string;
  url: string | null;
  summary: string | null;
  published: string | null;
  fetched_at: string | null;
  body?: string | null;
  content_type?: string | null;
  ai_summary?: string | null;
  bias_score?: number | null;
  bias_label?: string | null;
  bias_reasoning?: string | null;
  confidence_score?: number | null;
}

export interface FetchLog {
  id: string;
  outlet: string | null;
  run_at: string | null;
  articles_new: number | null;
  articles_skip: number | null;
  status: string | null;
  error_message: string | null;
  run_id: string | null;
}
