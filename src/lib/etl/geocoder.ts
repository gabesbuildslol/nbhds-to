/**
 * geocoder.ts
 *
 * Geocodes address strings and intersections to lat/lng using the
 * Google Maps Geocoding API, with a Supabase-backed cache to avoid
 * re-geocoding the same location twice.
 *
 * Cost context: Google Maps Geocoding is $5/1000 requests.
 * The 311 dataset has ~200K unique intersections across all years.
 * Caching aggressively is essential — budget ~$1K for the initial
 * historical load, then near-zero for daily incremental updates.
 */

import { createClient } from "@supabase/supabase-js";

const GEOCODING_API = "https://maps.googleapis.com/maps/api/geocode/json";

const supabase = createClient(
  process.env.SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_KEY!
);

export interface GeoPoint {
  lat: number;
  lng: number;
}

/**
 * Geocode any address string with cache-first lookup.
 * Constrains results to Toronto, ON to avoid false matches.
 */
export async function geocodeAddress(
  address: string
): Promise<GeoPoint | null> {
  const normalized = normalizeAddressKey(address);
  if (!normalized) return null;

  // 1. Check cache
  const cached = await getCached(normalized);
  if (cached) return cached;

  // 2. Hit Google Geocoding API
  const point = await callGoogleGeocoding(address);
  if (!point) return null;

  // 3. Store in cache
  await setCached(normalized, address, point);
  return point;
}

/**
 * Geocode a 311-style intersection string.
 * Format in dataset: "YONGE ST / BLOOR ST W" or "YONGE ST || BLOOR ST W"
 *
 * Strategy: convert to "Yonge St and Bloor St W, Toronto, ON"
 * Google handles intersections well in this format.
 */
export async function geocodeIntersection(
  raw: string
): Promise<GeoPoint | null> {
  if (!raw || raw.trim() === "") return null;

  // Normalize separators
  const cleaned = raw
    .replace(/\s*\|\|\s*/g, " and ")
    .replace(/\s*\/\s*/g, " and ")
    .replace(/\s+/g, " ")
    .trim();

  return geocodeAddress(cleaned);
}

/**
 * Batch geocode a list of unique intersections.
 * Respects rate limits — Google allows 50 req/s on standard plan.
 * Uses a concurrency limit to stay safe.
 */
export async function batchGeocode(
  addresses: string[],
  concurrency = 10
): Promise<Map<string, GeoPoint | null>> {
  const results = new Map<string, GeoPoint | null>();
  const unique = [...new Set(addresses)];

  // Process in chunks
  for (let i = 0; i < unique.length; i += concurrency) {
    const chunk = unique.slice(i, i + concurrency);
    const settled = await Promise.allSettled(
      chunk.map(async (addr) => {
        const point = await geocodeAddress(addr);
        return { addr, point };
      })
    );

    for (const result of settled) {
      if (result.status === "fulfilled") {
        results.set(result.value.addr, result.value.point);
      } else {
        results.set(chunk[settled.indexOf(result)], null);
      }
    }

    // Small delay between chunks to respect rate limits
    if (i + concurrency < unique.length) {
      await new Promise((r) => setTimeout(r, 100));
    }
  }

  return results;
}

// ── Private helpers ────────────────────────────────────────────────────────

function normalizeAddressKey(address: string): string {
  return address.toLowerCase().replace(/\s+/g, " ").trim();
}

async function getCached(key: string): Promise<GeoPoint | null> {
  const { data } = await supabase
    .from("geocode_cache")
    .select("lat, lng")
    .eq("address_key", key)
    .single();
  return data ? { lat: data.lat, lng: data.lng } : null;
}

async function setCached(
  key: string,
  raw: string,
  point: GeoPoint
): Promise<void> {
  const { error } = await supabase.from("geocode_cache").upsert(
    { address_key: key, address_raw: raw, lat: point.lat, lng: point.lng },
    { onConflict: "address_key" }
  );
  if (error) {
    console.error(`[setCached] Supabase cache write failed for "${key}":`, error);
  }
}

async function callGoogleGeocoding(address: string): Promise<GeoPoint | null> {
  const params = new URLSearchParams({
    address: `${address}, Toronto, ON, Canada`,
    key: process.env.GOOGLE_MAPS_API_KEY!,
    components: "country:CA|administrative_area:ON",
    region: "ca",
  });

  const res = await fetch(`${GEOCODING_API}?${params}`);
  if (!res.ok) {
    console.error(`Geocoding HTTP error: ${res.status} for "${address}"`);
    return null;
  }

  const data = await res.json();

  if (data.status === "ZERO_RESULTS" || !data.results?.length) return null;

  if (data.status !== "OK") {
    console.error(`Geocoding API error: ${data.status} for "${address}"`);
    return null;
  }

  const { lat, lng } = data.results[0].geometry.location;
  return { lat, lng };
}
