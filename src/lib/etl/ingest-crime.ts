/**
 * ingest-crime.ts
 *
 * ETL job for the Toronto Neighbourhood Crime Rates dataset.
 *
 * Strategy:
 * 1. Fetch all datastore-active resources from CKAN
 * 2. Stream records page-by-page from every resource
 * 3. Map fields using known name variants (shapefile-truncated vs. labelled)
 * 4. Upsert into Supabase — idempotent via (neighbourhood_id, year)
 *
 * No geocoding required — data is neighbourhood-level with polygon geometry
 * supplied by the source dataset.
 *
 * Field name note: Toronto's CKAN exports may use truncated DBF-style names
 * (e.g. "ASSAULT_RAT") or friendlier labelled names (e.g. "Assault_Rate").
 * The field() / numericField() helpers try each known variant in order.
 */

import { createClient } from "@supabase/supabase-js";
import { getPackageResources, paginateCKAN, CKANRecord } from "./ckan";

const PACKAGE_ID = "neighbourhood-crime-rates";
const UPSERT_BATCH = 500;

const supabase = createClient(
  process.env.SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_KEY!
);

export interface IngestResult {
  inserted: number;
  skipped: number;
  errors: number;
}

export async function ingestCrimeRates(): Promise<IngestResult> {
  const resources = await getPackageResources(PACKAGE_ID);
  const active = resources.filter((r) => r.datastore_active);

  if (!active.length) throw new Error("No active crime rates resources found");

  console.log(`[crime] Found ${active.length} active resource(s)`);

  let inserted = 0;
  let skipped = 0;
  let errors = 0;

  for (const resource of active) {
    console.log(`[crime] Processing resource: ${resource.name} (${resource.id})`);

    const rows: CrimeRateRow[] = [];

    const YEARS = [2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025];

    for await (const batch of paginateCKAN(resource.id)) {
      for (const r of batch) {
        const neighbourhood = field(r, "AREA_NAME");
        const neighbourhood_id = field(r, "HOOD_ID");

        if (!neighbourhood_id) {
          skipped++;
          continue;
        }

        const geom = null;
        const ingested_at = new Date().toISOString();

        for (const year of YEARS) {
          const assault_rate = numericField(r, `ASSAULT_RATE_${year}`);
          const auto_theft_rate = numericField(r, `AUTOTHEFT_RATE_${year}`);
          const break_enter_rate = numericField(r, `BREAKENTER_RATE_${year}`);
          const robbery_rate = numericField(r, `ROBBERY_RATE_${year}`);
          const shooting_rate = numericField(r, `SHOOTING_RATE_${year}`);
          const homicide_rate = numericField(r, `HOMICIDE_RATE_${year}`);

          // Skip years where all rate fields are null (sparse data)
          if (
            assault_rate === null &&
            auto_theft_rate === null &&
            break_enter_rate === null &&
            robbery_rate === null &&
            shooting_rate === null &&
            homicide_rate === null
          ) {
            skipped++;
            continue;
          }

          rows.push({
            neighbourhood,
            neighbourhood_id,
            year,
            assault_rate,
            auto_theft_rate,
            break_enter_rate,
            robbery_rate,
            shooting_rate,
            homicide_rate,
            geom,
            ingested_at,
          });
        }
      }
    }

    console.log(`[crime] Upserting ${rows.length} rows from "${resource.name}"`);

    for (let i = 0; i < rows.length; i += UPSERT_BATCH) {
      const batch = rows.slice(i, i + UPSERT_BATCH);
      const { error } = await supabase
        .from("crime_rates")
        .upsert(batch, { onConflict: "neighbourhood_id,year", ignoreDuplicates: false });

      if (error) {
        console.error("[crime] Upsert error:", error.message);
        errors += batch.length;
      } else {
        inserted += batch.length;
      }
    }
  }

  console.log(
    `[crime] Done — inserted/updated: ${inserted}, skipped: ${skipped}, errors: ${errors}`
  );
  return { inserted, skipped, errors };
}

// ── Types ──────────────────────────────────────────────────────────────────

interface CrimeRateRow {
  neighbourhood: string | null;
  neighbourhood_id: string;
  year: number;
  assault_rate: number | null;
  auto_theft_rate: number | null;
  break_enter_rate: number | null;
  robbery_rate: number | null;
  shooting_rate: number | null;
  homicide_rate: number | null;
  geom: string | null; // WKT/EWKT polygon, or null if not present
  ingested_at: string;
}

// ── Helpers ────────────────────────────────────────────────────────────────

/**
 * Read a field from a CKAN record, trying multiple known name variants.
 * Returns the first non-empty string value found, or null.
 */
function field(record: CKANRecord, ...names: string[]): string | null {
  for (const name of names) {
    const val = record[name];
    if (val !== undefined && val !== null && val !== "") {
      return String(val).trim();
    }
  }
  return null;
}

/**
 * Like field(), but parses the result as a float.
 * Returns null if no variant is found or the value is not a valid number.
 */
function numericField(record: CKANRecord, ...names: string[]): number | null {
  const raw = field(record, ...names);
  if (raw === null) return null;
  const n = parseFloat(raw);
  return isNaN(n) ? null : n;
}

/**
 * Extract polygon geometry from a CKAN record.
 *
 * Toronto's CKAN datastore exposes geometry as a JSON string under a
 * "geometry" key (GeoJSON). We convert it to an EWKT string for PostGIS.
 * Falls back to a raw WKT "geom" field if present.
 */
function parseGeom(record: CKANRecord): string | null {
  // GeoJSON object or string stored under "geometry"
  const raw = record["geometry"];
  if (raw) {
    try {
      const geo =
        typeof raw === "string" ? (JSON.parse(raw) as GeoJSON) : (raw as GeoJSON);
      if (geo && geo.type) {
        return `SRID=4326;${geojsonToWkt(geo)}`;
      }
    } catch {
      // fall through to raw WKT check
    }
  }

  // Some exports provide a pre-built WKT string
  const wkt = field(record, "geom", "WKT", "wkt");
  if (wkt) return wkt.startsWith("SRID=") ? wkt : `SRID=4326;${wkt}`;

  return null;
}

// ── Minimal GeoJSON → WKT converter (Polygon / MultiPolygon only) ──────────

interface GeoJSON {
  type: string;
  coordinates?: unknown;
}

function geojsonToWkt(geo: GeoJSON): string {
  switch (geo.type) {
    case "Polygon":
      return `POLYGON(${ringListToWkt(geo.coordinates as number[][][])})`;
    case "MultiPolygon":
      return `MULTIPOLYGON(${(geo.coordinates as number[][][][])
        .map((poly) => `(${ringListToWkt(poly)})`)
        .join(",")})`;
    default:
      throw new Error(`Unsupported geometry type: ${geo.type}`);
  }
}

function ringListToWkt(rings: number[][][]): string {
  return rings
    .map((ring) => `(${ring.map((c) => `${c[0]} ${c[1]}`).join(",")})`)
    .join(",");
}

/** Alias for ingestCrimeRates — used by the historical-load script. */
export const ingestCrime = ingestCrimeRates;
