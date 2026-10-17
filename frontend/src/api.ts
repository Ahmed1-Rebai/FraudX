// Typed access to the FraudX API (served under /api by nginx or the Vite proxy).

export type Level = "LOW" | "MEDIUM" | "HIGH";
export type Status = "OPEN" | "CONFIRMED_FRAUD" | "FALSE_POSITIVE" | "DISMISSED";

export interface Overview {
  window: string;
  scored: number | null;
  by_level: Partial<Record<Level, number>>;
  alerts: number | null;
  transactions_per_s: number | null;
  avg_risk_score: number | null;
  throughput: { t: number; value: number }[];
  investigations: Partial<Record<Status, number>>;
  confirmed_precision: number | null;
  metrics_available: boolean;
}

export interface LiveEvent {
  transaction_id: string;
  customer_id: string;
  event_time: string;
  amount: number;
  currency: string;
  transaction_type: string;
  country: string | null;
  score: number;
  level: Level;
  components: Record<string, number>;
  rule_hits: string[];
  scored_at: string;
}

export interface RiskEvent {
  transaction_id: string;
  customer_id: string;
  event_time: string;
  amount: number;
  transaction_type: string;
  risk_score: number;
  risk_level: Level;
  rule_hits: string[] | null;
}

export interface Transaction {
  transaction_id: string;
  customer_id: string;
  event_time: string;
  source_account: string;
  destination_account: string;
  transaction_type: string;
  amount: number;
  currency: string;
  device_id: string | null;
  ip_address: string | null;
  country: string | null;
  city: string | null;
  risk_score: number | null;
  risk_level: Level | null;
  ml_score: number | null;
  anomaly_score: number | null;
  graph_score: number | null;
  rule_hits: string[] | null;
}

export interface Explanation {
  summary: string;
  reasons: string[];
  reason_evidence: string[][];
  generator: string;
  created_at: string;
}

export interface Investigation {
  transaction_id: string;
  customer_id: string;
  event_time: string;
  status: Status;
  risk: {
    score: number;
    level: Level;
    components: Record<string, number>;
    rule_hits: string[];
    missing_signals: string[];
  };
  explanation: Explanation;
  created_at: string;
  updated_at: string;
  analyst_note: string | null;
  models?: Record<string, string>;
  evidence?: Record<string, unknown>;
}

export interface ExplanationView {
  transaction_id: string;
  summary: string;
  generator: string;
  reasons: { text: string; evidence: Record<string, unknown> }[];
}

export interface Device {
  device_id: string;
  uses: number;
  first_seen: string;
  last_seen: string;
  shared_with_count: number;
  shared_with: string[];
}

export interface Counterparty {
  account: string;
  counterparty: string;
  kind: string | null;
  direction: "in" | "out";
  count: number;
  total_amount: number;
  last_seen: string;
}

export interface Network {
  customer_id: string;
  devices: Device[];
  counterparties: Counterparty[];
}

export interface Profile {
  customer_id: string;
  country: string | null;
  accounts: string[];
  devices: string[];
  known_ips: string[];
  countries: string[];
  first_seen: string | null;
  last_seen: string | null;
  risk_profile: { score: number; level: Level; transaction_id: string } | null;
}

export class ApiError extends Error {
  constructor(
    readonly status: number,
    message: string,
  ) {
    super(message);
  }
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const resp = await fetch(`/api${path}`, {
    ...init,
    headers: { "content-type": "application/json", ...init?.headers },
  });
  if (!resp.ok) {
    let detail = resp.statusText;
    try {
      detail = (await resp.json()).detail ?? detail;
    } catch {
      /* not JSON */
    }
    throw new ApiError(resp.status, typeof detail === "string" ? detail : JSON.stringify(detail));
  }
  return resp.json() as Promise<T>;
}

const q = (params: Record<string, string | number | undefined>) =>
  new URLSearchParams(
    Object.entries(params).filter(([, v]) => v !== undefined && v !== "") as [string, string][],
  ).toString();

export const api = {
  overview: () => request<Overview>("/stats/overview"),
  alerts: (day: string, level?: Level) =>
    request<{ day: string; events: RiskEvent[] }>(`/fraud/alerts?${q({ day, level, limit: 200 })}`),
  investigations: (status: Status) =>
    request<Investigation[]>(`/fraud/investigations?${q({ status, limit: 200 })}`),
  investigation: (tx: string) => request<Investigation>(`/fraud/${encodeURIComponent(tx)}`),
  explanation: (tx: string) =>
    request<ExplanationView>(`/fraud/${encodeURIComponent(tx)}/explanation`),
  decide: (tx: string, status: Status, note?: string) =>
    request<Investigation>(`/fraud/${encodeURIComponent(tx)}`, {
      method: "PATCH",
      body: JSON.stringify({ status, note: note || null }),
    }),
  transaction: (tx: string) => request<Transaction>(`/transactions/${encodeURIComponent(tx)}`),
  profile: (customer: string) => request<Profile>(`/customers/${encodeURIComponent(customer)}`),
  network: (customer: string) =>
    request<Network>(`/customers/${encodeURIComponent(customer)}/network?limit=20`),
};

/** Subscribe to the live stream of scored transactions; returns an unsubscribe function. */
export function subscribeScored(
  onEvent: (event: LiveEvent) => void,
  onStatus: (connected: boolean) => void,
): () => void {
  const source = new EventSource("/api/stream/scored");
  source.onopen = () => onStatus(true);
  source.onerror = () => onStatus(false); // EventSource reconnects by itself
  source.onmessage = (msg) => onEvent(JSON.parse(msg.data) as LiveEvent);
  return () => source.close();
}

export const fmt = {
  int: (n: number | null | undefined) => (n == null ? "–" : Math.round(n).toLocaleString("en")),
  compact: (n: number | null | undefined) =>
    n == null
      ? "–"
      : new Intl.NumberFormat("en", { notation: "compact", maximumFractionDigits: 1 }).format(n),
  money: (n: number, currency: string) =>
    `${n.toLocaleString("en", { minimumFractionDigits: 2, maximumFractionDigits: 2 })} ${currency}`,
  pct: (n: number | null | undefined, digits = 0) =>
    n == null ? "–" : `${(n * 100).toFixed(digits)}%`,
  time: (iso: string) =>
    new Date(iso).toLocaleString("en-GB", {
      dateStyle: "short",
      timeStyle: "medium",
      timeZone: "UTC",
    }),
};
