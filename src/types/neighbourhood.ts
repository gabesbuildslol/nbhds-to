/**
 * Shared TypeScript types that exactly mirror the JSON response shape
 * returned by GET /api/neighbourhood.
 */

export type TimeWindow = "90d" | "1y" | "3y" | "all";

export type ServiceRequestStatus = "Open" | "Closed" | "In Progress" | "Cancelled";

export type DineSafeResult = "Pass" | "Conditional Pass" | "Closed";

// ── Service requests ───────────────────────────────────────────────────────

export interface ServiceRequestCategory {
  category: string;
  label: string;
  count: number;
}

export interface RecentServiceRequest {
  created_at: string;
  category: string;
  label: string;
  raw_type: string;
  status: ServiceRequestStatus;
  ward: string | null;
  distance_m: number;
}

export interface ServiceRequestSummary {
  total: number;
  by_category: ServiceRequestCategory[];
  recent: RecentServiceRequest[];
}

// ── Building permits ───────────────────────────────────────────────────────

export interface RecentPermit {
  permit_type: string;
  description: string | null;
  status: string;
  issued_date: string | null;
  address: string;
  distance_m: number;
}

export interface BuildingPermitSummary {
  total_count: number;
  active_count: number;
  recent: RecentPermit[];
}

// ── DineSafe ───────────────────────────────────────────────────────────────

export interface RecentInspection {
  establishment: string;
  estab_type: string | null;
  inspection_date: string;
  result: DineSafeResult;
  severity: string | null;
  distance_m: number;
}

export interface DineSafeSummary {
  total_establishments: number;
  failed_last_90d: number;
  /** null when there are no establishments in the radius */
  pass_rate: number | null;
  recent_inspections: RecentInspection[];
}

// ── Crime ──────────────────────────────────────────────────────────────────

export interface CrimeRates {
  assault: number | null;
  auto_theft: number | null;
  break_enter: number | null;
  robbery: number | null;
}

export interface CrimeSummary {
  neighbourhood: string;
  year: number;
  rates: CrimeRates;
}

// ── Top-level response ─────────────────────────────────────────────────────

export interface NeighbourhoodResponse {
  address: {
    formatted: string;
    lat: number;
    lng: number;
  };
  radius_m: number;
  window: TimeWindow;
  is_paid: boolean;
  service_requests: ServiceRequestSummary;
  building_permits: BuildingPermitSummary;
  dinesafe: DineSafeSummary;
  /** null when the queried point falls outside any known neighbourhood polygon */
  crime: CrimeSummary | null;
}
