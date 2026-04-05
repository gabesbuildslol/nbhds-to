/**
 * ingest-permits.ts
 *
 * ETL job for the Toronto Building Permits (Active Permits) dataset.
 *
 * Strategy:
 * 1. Fetch the most recent active resource from CKAN
 * 2. Collect all records into memory
 * 3. Batch geocode unique addresses (cache-first)
 * 4. Upsert into Supabase — idempotent via source_id
 *
 * Field name note: permit field names vary across dataset versions.
 * The field() helper tries each known variant in order.
 */

import { createClient } from "@supabase/supabase-js";
import {
  getPackageResources,
  getMostRecentResource,
  paginateCKAN,
  CKANRecord,
} from "./ckan";
import { batchGeocode } from "./geocoder";

const PACKAGE_ID = "building-permits-active-permits";
const UPSERT_BATCH = 500;

const supabase = createClient(
  process.env.SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_KEY!
);

import type { IngestResult } from "./ingest-311";

export async function ingestPermits(
  options: {
    /** Cap total records processed — useful for smoke tests */
    limit?: number;
  } = {}
): Promise<IngestResult> {
  const resources = await getPackageResources(PACKAGE_ID);
  const target = getMostRecentResource(resources);

  if (!target) throw new Error("No active building permits resource found");

  console.log(`[permits] Processing resource: ${target.name} (${target.id})`);

  let allRecords: CKANRecord[] = [];
  for await (const batch of paginateCKAN(target.id)) {
    allRecords.push(...batch);
    if (options.limit && allRecords.length >= options.limit) break;
  }
  if (options.limit) allRecords = allRecords.slice(0, options.limit);

  console.log(`[permits] Fetched ${allRecords.length} records from CKAN`);

  // Collect unique addresses for batch geocoding
  const uniqueAddresses = new Set<string>();
  for (const r of allRecords) {
    const address = buildAddress(r);
    if (address) uniqueAddresses.add(address);
  }

  console.log(
    `[permits] Batch geocoding ${uniqueAddresses.size} unique addresses...`
  );
  const geocodeMap = await batchGeocode([...uniqueAddresses]);

  let inserted = 0;
  let skipped = 0;
  let errors = 0;

  const rows: BuildingPermitRow[] = [];

  for (const r of allRecords) {
    const address = buildAddress(r);
    const point = address ? geocodeMap.get(address) : null;

    if (!point) {
      skipped++;
      continue; // Only index geocoded records
    }

    rows.push({
      source_id: String(r._id),
      permit_type: field(r, "PERMIT_TYPE", "Permit Type"),
      description: field(r, "DESCRIPTION", "Work Description"),
      status: field(r, "STATUS", "Permit Status"),
      issued_date: field(r, "ISSUED_DATE", "Date Issued"),
      address,
      // WKT format for PostGIS — note: PostGIS uses (lng, lat) order
      geom: `SRID=4326;POINT(${point.lng} ${point.lat})`,
    });
  }

  // Upsert in batches
  for (let i = 0; i < rows.length; i += UPSERT_BATCH) {
    const batch = rows.slice(i, i + UPSERT_BATCH);
    const { error } = await supabase
      .from("building_permits")
      .upsert(batch, { onConflict: "source_id", ignoreDuplicates: true });

    if (error) {
      console.error("[permits] Upsert error:", error.message);
      errors += batch.length;
    } else {
      inserted += batch.length;
    }
  }

  console.log(
    `[permits] Done — inserted: ${inserted}, skipped (no geocode): ${skipped}, errors: ${errors}`
  );
  return { inserted, skipped, errors };
}

// ── Types ──────────────────────────────────────────────────────────────────

interface BuildingPermitRow {
  source_id: string;
  permit_type: string | null;
  description: string | null;
  status: string | null;
  issued_date: string | null;
  address: string | null;
  geom: string; // WKT POINT
}

// ── Helpers ────────────────────────────────────────────────────────────────

/**
 * Build a full street address from component fields:
 * STREET_NUM + STREET_NAME + STREET_TYPE [+ STREET_DIRECTION]
 * Returns null if both number and name are absent.
 */
function buildAddress(record: CKANRecord): string | null {
  const num = field(record, "STREET_NUM");
  const name = field(record, "STREET_NAME");
  if (!num && !name) return null;
  const type = field(record, "STREET_TYPE");
  const dir = field(record, "STREET_DIRECTION");
  return [num, name, type, dir].filter(Boolean).join(" ");
}

/**
 * Read a field from a CKAN record, trying multiple known field name variants.
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
