/**
 * /app/api/neighbourhood/route.ts
 *
 * The core API endpoint. Takes an address, geocodes it, and runs
 * four PostGIS spatial queries in parallel against Supabase.
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

  // Run all 4 spatial queries in parallel
  const [requestsRes, permitsRes, dinesafeRes, crimeRes] = await Promise.all([
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
      p_since: since,
    }),
    supabase.rpc("get_crime_for_point", {
      p_lat: lat,
      p_lng: lng,
    }),
  ]);

  // Log any DB errors but don't fail the request — return partial data
  for (const [name, res] of [
    ["service_requests", requestsRes],
    ["permits", permitsRes],
    ["dinesafe", dinesafeRes],
    ["crime", crimeRes],
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
      crime: formatCrime(crimeRes.data ?? []),
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

function formatCrime(rows: any[]) {
  if (!rows.length) return null;
  const r = rows[0]; // Point-in-polygon returns the enclosing neighbourhood
  return {
    neighbourhood: r.neighbourhood,
    year: r.year,
    rates: {
      assault: r.assault_rate,
      auto_theft: r.auto_theft_rate,
      break_enter: r.break_enter_rate,
      robbery: r.robbery_rate,
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
