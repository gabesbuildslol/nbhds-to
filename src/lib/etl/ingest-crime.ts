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

    for await (const batch of paginateCKAN(resource.id)) {
      for (const r of batch) {
        const neighbourhood = field(r, "AREA_NAME", "Neighbourhood");
        const neighbourhood_id = field(r, "AREA_ID", "Hood_ID", "HOOD_ID");
        const yearRaw = field(r, "REPORT_YEAR", "Year");

        // Both neighbourhood_id and year are required for the unique constraint
        if (!neighbourhood_id || !yearRaw) {
          skipped++;
          continue;
        }

        const year = parseInt(yearRaw, 10);
        if (isNaN(year)) {
          skipped++;
          continue;
        }

        rows.push({
          neighbourhood,
          neighbourhood_id,
          year,
          assault_rate: numericField(r, "ASSAULT_RAT", "Assault_Rate"),
          auto_theft_rate: numericField(r, "AUTOTHEFT_R", "AutoTheft_Rate", "Auto_Theft_Rate"),
          break_enter_rate: numericField(r, "BREAKENTER_", "BreakEnter_Rate", "Break_Enter_Rate"),
          robbery_rate: numericField(r, "ROBBERY_RAT", "Robbery_Rate"),
          shooting_rate: numericField(r, "SHOOTING_RA", "Shootings_Rate", "Shooting_Rate"),
          homicide_rate: numericField(r, "HOMICIDE_RA", "Homicide_Rate"),
          geom: parseGeom(r),
          ingested_at: new Date().toISOString(),
        });
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
