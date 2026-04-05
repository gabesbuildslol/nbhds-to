/**
 * /app/api/neighbourhood/route.ts
 *
 * The core API endpoint. Takes an address, geocodes it, and runs
 * spatial queries in parallel against Supabase.
 *
 * Freemium gate: the `window` param controls how far back to look.
 * Free tier gets 90d. Paid tiers get 1y / 3y / all (verified via
 * Stripe session token in the Authorization header).
 *
 * This is also the route that powers the embeddable widget —
 * /api/widget/[slug] calls this internally with window=90d.
 */

import { NextRequest, NextResponse } from "next/server";
import { createClient } from "@supabase/supabase-js";
import { geocodeAddress } from "@/lib/etl/geocoder";
import { CATEGORY_LABELS, ServiceCategory } from "@/lib/etl/normalizer";

const supabase = createClient(
  process.env.SUPABASE_URL!,
  process.env.SUPABASE_ANON_KEY!
);

// Paid window options in days
const WINDOWS: Record<string, number> = {
  "90d": 90,
  "1y": 365,
  "3y": 1095,
  all: 36500, // ~100 years — effectively all records
};

const FREE_WINDOW = "90d";
const DEFAULT_RADIUS_M = 500;
const MAX_RADIUS_M = 1000;

// DineSafe inspections happen at most once or twice a year per establishment.
// Using the same 90-day window as 311 requests filters out all inspections
// that are more than 3 months old, which is almost always everything.
// Use a fixed 3-year lookback so recent-but-not-last-90-days inspections show.
const DINESAFE_LOOKBACK_DAYS = 1095;

// ── Neighbourhood centroid cache (for crime lookup) ─────────────────────────

const CRIME_CKAN_RESOURCE = "d4160604-9f3e-4589-8821-9fd70fa350b3";

interface NeighbourhoodCentroid {
  neighbourhood_id: string;
  neighbourhood: string;
  lat: number;
  lng: number;
}

// Module-level caches — persist for the lifetime of the server process
let _centroids: NeighbourhoodCentroid[] | null = null;

// Most-recent crime row keyed by neighbourhood_id, fetched once from Supabase
let _crimeRates: Map<string, Record<string, unknown>> | null = null;

async function getCrimeRatesCache(): Promise<Map<string, Record<string, unknown>> | null> {
  if (_crimeRates) return _crimeRates;

  const { data, error } = await supabase
    .from("crime_rates")
    .select(
      "neighbourhood, neighbourhood_id, year, assault_rate, auto_theft_rate, break_enter_rate, robbery_rate, shooting_rate"
    )
    .order("year", { ascending: false });

  if (error || !data) {
    console.error("[api/neighbourhood] Failed to fetch crime rates cache:", error);
    return null;
  }

  // Group by neighbourhood_id — because rows are ordered year desc, the first
  // row for each id is already the most recent year.
  const map = new Map<string, Record<string, unknown>>();
  for (const row of data) {
    const id = row.neighbourhood_id as string;
    if (!map.has(id)) {
      map.set(id, row as Record<string, unknown>);
    }
  }

  _crimeRates = map;
  return _crimeRates;
}

async function getNeighbourhoodCentroids(): Promise<NeighbourhoodCentroid[] | null> {
  if (_centroids) return _centroids;

  try {
    const url = `https://ckan0.cf.opendata.inter.prod-toronto.ca/api/3/action/datastore_search?resource_id=${CRIME_CKAN_RESOURCE}&limit=200`;
    const res = await fetch(url, { next: { revalidate: 86400 } }); // cache for 24h in Next.js
    if (!res.ok) return null;

    const data = await res.json();
    const records: Record<string, unknown>[] = data?.result?.records ?? [];

    _centroids = records
      .map((r) => {
        const id = r.HOOD_ID ? String(r.HOOD_ID) : null;
        const name = typeof r.AREA_NAME === "string" ? r.AREA_NAME : null;
        if (!id || !name) return null;

        const centroid = r.geometry ? polygonCentroid(r.geometry) : null;
        if (!centroid) return null;

        return { neighbourhood_id: id, neighbourhood: name, ...centroid };
      })
      .filter((c): c is NeighbourhoodCentroid => c !== null);
  } catch (err) {
    console.error("[api/neighbourhood] Failed to fetch neighbourhood centroids:", err);
    return null;
  }

  return _centroids;
}

/** Compute the centroid of a GeoJSON Polygon or MultiPolygon as lat/lng. */
function polygonCentroid(rawGeom: unknown): { lat: number; lng: number } | null {
  try {
    const geo =
      typeof rawGeom === "string"
        ? (JSON.parse(rawGeom) as { coordinates: unknown })
        : (rawGeom as { coordinates: unknown });

    const coords: [number, number][] = [];
    function collect(c: unknown): void {
      if (!Array.isArray(c)) return;
      if (typeof c[0] === "number") {
        coords.push(c as [number, number]);
      } else {
        c.forEach(collect);
      }
    }
    collect(geo.coordinates);
    if (!coords.length) return null;

    const lng = coords.reduce((s, c) => s + c[0], 0) / coords.length;
    const lat = coords.reduce((s, c) => s + c[1], 0) / coords.length;
    return { lat, lng };
  } catch {
    return null;
  }
}

/** Find the nearest Toronto neighbourhood and return its most recent crime data.
 *
 * Both the CKAN neighbourhood centroids and the Supabase crime_rates rows are
 * cached in module scope so this is a pure in-memory lookup after the first
 * request — no extra network/DB round-trip per API call.
 */
async function fetchCrimeForPoint(
  lat: number,
  lng: number
): Promise<Record<string, unknown> | null> {
  const [centroids, crimeRates] = await Promise.all([
    getNeighbourhoodCentroids(),
    getCrimeRatesCache(),
  ]);

  if (!centroids?.length || !crimeRates) return null;

  // Euclidean distance in degrees — accurate enough for nearest-match within a city
  let nearest = centroids[0];
  let bestDist = Infinity;
  for (const c of centroids) {
    const d = (c.lat - lat) ** 2 + (c.lng - lng) ** 2;
    if (d < bestDist) {
      bestDist = d;
      nearest = c;
    }
  }

  return crimeRates.get(nearest.neighbourhood_id) ?? null;
}

export async function GET(request: NextRequest) {
  const { searchParams } = new URL(request.url);

  const address = searchParams.get("address");
  const radius = Math.min(
    Number(searchParams.get("radius") ?? DEFAULT_RADIUS_M),
    MAX_RADIUS_M
  );

  if (!address) {
    return NextResponse.json({ error: "address is required" }, { status: 400 });
  }

  // Resolve window — default to free tier
  const requestedWindow = searchParams.get("window") ?? FREE_WINDOW;
  const isPaid = await verifyPaidSession(request);
  const window = isPaid && WINDOWS[requestedWindow] ? requestedWindow : FREE_WINDOW;
  const lookbackDays = WINDOWS[window];
  const since = new Date(Date.now() - lookbackDays * 86400000).toISOString();

  // Geocode the address
  const point = await geocodeAddress(address);
  if (!point) {
    return NextResponse.json(
      { error: "Address not found or outside Toronto" },
      { status: 404 }
    );
  }

  const { lat, lng } = point;

  // DineSafe uses a fixed 3-year lookback — inspections happen at most annually,
  // so the tier-based 90-day window would exclude almost all establishments.
  const dinesafeSince = new Date(
    Date.now() - DINESAFE_LOOKBACK_DAYS * 86400000
  ).toISOString();

  // Run spatial queries in parallel
  const [requestsRes, permitsRes, dinesafeRes, crimeData] = await Promise.all([
    supabase.rpc("get_service_requests_near", {
      p_lat: lat,
      p_lng: lng,
      p_radius_m: radius,
      p_since: since,
    }),
    supabase.rpc("get_permits_near", {
      p_lat: lat,
      p_lng: lng,
      p_radius_m: radius,
    }),
    supabase.rpc("get_dinesafe_near", {
      p_lat: lat,
      p_lng: lng,
      p_radius_m: radius,
      p_since: dinesafeSince,
    }),
    // get_crime_for_point uses polygon intersection which fails when geom is null.
    // Instead, fetch centroids from the CKAN source and find the nearest neighbourhood.
    fetchCrimeForPoint(lat, lng),
  ]);

  // Log any DB errors but don't fail the request — return partial data
  for (const [name, res] of [
    ["service_requests", requestsRes],
    ["permits", permitsRes],
    ["dinesafe", dinesafeRes],
  ] as const) {
    if (res.error) console.error(`[api/neighbourhood] ${name} error:`, res.error);
  }

  return NextResponse.json(
    {
      address: {
        formatted: address,
        lat,
        lng,
      },
      radius_m: radius,
      window,
      is_paid: isPaid,
      service_requests: formatServiceRequests(requestsRes.data ?? []),
      building_permits: formatPermits(permitsRes.data ?? []),
      dinesafe: formatDineSafe(dinesafeRes.data ?? []),
      crime: formatCrime(crimeData),
    },
    {
      headers: {
        // Cache for 1 hour — data refreshes daily anyway
        "Cache-Control": "public, s-maxage=3600, stale-while-revalidate=7200",
      },
    }
  );
}

// ── Formatting helpers ─────────────────────────────────────────────────────

function formatServiceRequests(rows: any[]) {
  // Aggregate by category
  const byCat: Record<string, number> = {};
  for (const r of rows) {
    byCat[r.category] = (byCat[r.category] ?? 0) + 1;
  }

  const byCategory = Object.entries(byCat)
    .map(([category, count]) => ({
      category,
      label: CATEGORY_LABELS[category as ServiceCategory] ?? category,
      count,
    }))
    .sort((a, b) => b.count - a.count);

  return {
    total: rows.length,
    by_category: byCategory,
    // Most recent 10 for the report feed
    recent: rows.slice(0, 10).map((r) => ({
      created_at: r.created_at,
      category: r.category,
      label: CATEGORY_LABELS[r.category as ServiceCategory] ?? r.category,
      raw_type: r.raw_type,
      status: r.status,
      ward: r.ward,
      distance_m: Math.round(r.distance_m),
    })),
  };
}

function formatPermits(rows: any[]) {
  const active = rows.filter((r) =>
    ["open", "issued", "pending"].includes(r.status?.toLowerCase())
  );

  return {
    total_count: rows.length,
    active_count: active.length,
    recent: rows.slice(0, 10).map((r) => ({
      permit_type: r.permit_type,
      description: r.description,
      status: r.status,
      issued_date: r.issued_date,
      address: r.address,
      distance_m: Math.round(r.distance_m),
    })),
  };
}

function formatDineSafe(rows: any[]) {
  const establishments = new Map<string, any>();

  // Deduplicate by establishment — keep most recent inspection per place
  for (const r of rows) {
    const key = r.source_id;
    if (!establishments.has(key)) {
      establishments.set(key, r);
    }
  }

  const all = [...establishments.values()];
  const failed = all.filter((r) =>
    ["conditional pass", "closed"].includes(r.result?.toLowerCase())
  );

  return {
    total_establishments: all.length,
    failed_last_90d: failed.length,
    pass_rate:
      all.length > 0
        ? Math.round(((all.length - failed.length) / all.length) * 100)
        : null,
    recent_inspections: rows.slice(0, 10).map((r) => ({
      establishment: r.establishment,
      estab_type: r.estab_type,
      inspection_date: r.inspection_date,
      result: r.result,
      severity: r.severity,
      distance_m: Math.round(r.distance_m),
    })),
  };
}

function formatCrime(row: Record<string, unknown> | null) {
  if (!row) return null;
  return {
    neighbourhood: row.neighbourhood,
    year: row.year,
    rates: {
      assault: row.assault_rate,
      auto_theft: row.auto_theft_rate,
      break_enter: row.break_enter_rate,
      robbery: row.robbery_rate,
    },
  };
}

// ── Auth ───────────────────────────────────────────────────────────────────

/**
 * Verify a paid Stripe session from the Authorization header.
 * Replace with your actual Stripe customer lookup logic.
 */
async function verifyPaidSession(request: NextRequest): Promise<boolean> {
  const auth = request.headers.get("authorization");
  if (!auth?.startsWith("Bearer ")) return false;

  // TODO: validate token against Stripe customer table in Supabase
  // const token = auth.slice(7)
  // const { data } = await supabase.from('paid_sessions').select().eq('token', token).single()
  // return !!data && new Date(data.expires_at) > new Date()

  return false; // Default to free tier until Stripe is wired up
}
