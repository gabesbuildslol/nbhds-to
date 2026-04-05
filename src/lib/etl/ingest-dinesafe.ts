/**
 * ingest-dinesafe.ts
 *
 * Daily ETL job for the Toronto DineSafe restaurant inspection dataset.
 *
 * Strategy:
 * 1. Fetch the active resource from CKAN
 * 2. Collect all records into memory
 * 3. Read Latitude/Longitude columns directly (no geocoding needed)
 * 4. Upsert into Supabase — idempotent via source_id
 */

import { createClient } from "@supabase/supabase-js";
import {
  getPackageResources,
  getMostRecentResource,
  paginateCKAN,
  CKANRecord,
} from "./ckan";

const PACKAGE_ID = "dinesafe";
const UPSERT_BATCH = 500; // rows per Supabase upsert

const supabase = createClient(
  process.env.SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_KEY!
);

export interface IngestResult {
  inserted: number;
  skipped: number;
  errors: number;
}

export async function ingestDineSafe(
  options: {
    /** Cap total records processed — useful for smoke tests */
    limit?: number;
  } = {}
): Promise<IngestResult> {
  const resources = await getPackageResources(PACKAGE_ID);
  const target = getMostRecentResource(resources);

  if (!target) throw new Error("No active DineSafe resources found");

  console.log(`[DineSafe] Processing resource: ${target.name} (${target.id})`);

  // Collect all records from this resource into memory first
  // so we can batch-geocode unique addresses efficiently
  let allRecords: CKANRecord[] = [];
  for await (const batch of paginateCKAN(target.id)) {
    allRecords.push(...batch);
    if (options.limit && allRecords.length >= options.limit) break;
  }
  if (options.limit) allRecords = allRecords.slice(0, options.limit);

  console.log(`[DineSafe] Fetched ${allRecords.length} records from CKAN`);

  let inserted = 0;
  let skipped = 0;
  let errors = 0;

  // Process and upsert in batches
  const rows: DineSafeRow[] = [];

  for (const r of allRecords) {
    const latStr = field(r, "Latitude");
    const lngStr = field(r, "Longitude");
    const lat = latStr ? parseFloat(latStr) : NaN;
    const lng = lngStr ? parseFloat(lngStr) : NaN;

    if (!latStr || !lngStr || isNaN(lat) || isNaN(lng)) {
      skipped++;
      continue;
    }

    rows.push({
      source_id: String(r._id),
      establishment: field(r, "Establishment Name"),
      estab_type: field(r, "Establishment Type"),
      inspection_date: field(r, "Inspection Date"),
      result: field(r, "Establishment Status"),
      severity: field(r, "Severity"),
      // WKT format for PostGIS — note: PostGIS uses (lng, lat) order
      geom: `SRID=4326;POINT(${lng} ${lat})`,
    });
  }

  // Upsert in batches
  for (let i = 0; i < rows.length; i += UPSERT_BATCH) {
    const batch = rows.slice(i, i + UPSERT_BATCH);
    const { error } = await supabase
      .from("dinesafe_inspections")
      .upsert(batch, { onConflict: "source_id", ignoreDuplicates: true });

    if (error) {
      console.error("[DineSafe] Upsert error:", error.message);
      errors += batch.length;
    } else {
      inserted += batch.length;
    }
  }

  console.log(
    `[DineSafe] Done — inserted: ${inserted}, skipped (no geocode): ${skipped}, errors: ${errors}`
  );
  return { inserted, skipped, errors };
}

// ── Types ─────────────────────────────────────────────────────────────────

interface DineSafeRow {
  source_id: string;
  establishment: string | null;
  estab_type: string | null;
  inspection_date: string | null;
  result: string | null;
  severity: string | null;
  geom: string; // WKT POINT
}

// ── Helpers ───────────────────────────────────────────────────────────────

/**
 * Read a field from a CKAN record, trying multiple known field name variants.
 * DineSafe field names may differ between dataset versions.
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
