/**
 * ingest-311.ts
 *
 * Daily ETL job for the Toronto 311 Service Requests dataset.
 *
 * Strategy:
 * 1. Fetch package resources from CKAN and filter for CSV/ZIP downloads
 * 2. Sort by name descending so the most recent year comes first
 * 3. Download each file, extract the CSV from ZIPs, and parse it
 * 4. Collect all unique intersections and batch-geocode them (cache-first)
 * 5. Normalize request types to clean categories
 * 6. Upsert into Supabase — idempotent via source_id
 *
 * Field name note: Toronto's 311 dataset has evolved over the years
 * and field names differ between annual files. The field() helper
 * handles known variations with case-insensitive fallback.
 */

import { createClient } from "@supabase/supabase-js";
import { getPackageResources } from "./ckan";
import { batchGeocode } from "./geocoder";
import { buildCategoryLookup, normalizeCategory } from "./normalizer";
import { parse as csvParseSync } from "csv-parse/sync";
import AdmZip from "adm-zip";

const PACKAGE_ID = "311-service-requests-customer-initiated";
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

export async function ingest311(
  options: {
    /** Only ingest records created after this date. Defaults to 7 days ago for daily runs. */
    since?: Date;
    /** Set true for the initial historical load — processes all years */
    fullLoad?: boolean;
    /** Cap total records processed — useful for smoke tests */
    limit?: number;
  } = {}
): Promise<IngestResult> {
  const since = options.since ?? new Date(Date.now() - 7 * 86400000);

  const resources = await getPackageResources(PACKAGE_ID);

  // Filter to CSV / ZIP file downloads (datastore_active is false for this dataset)
  const csvResources = resources
    .filter((r) => {
      const fmt = (r.format ?? "").toUpperCase();
      const url = (r.url ?? "").toLowerCase();
      return (
        fmt === "CSV" ||
        fmt === "ZIP" ||
        url.includes(".csv") ||
        url.includes(".zip")
      );
    })
    .sort((a, b) => b.name.localeCompare(a.name)); // descending → most recent year first

  if (!csvResources.length) throw new Error("No CSV/ZIP 311 resources found");

  // Full load: restrict to 2023-onwards to avoid downloading older annual ZIPs.
  // Incremental: only the most recent resource.
  const RECENT_YEARS = ["2023", "2024", "2025", "2026"];
  const targets = options.fullLoad
    ? csvResources.filter((r) => RECENT_YEARS.some((y) => r.name.includes(y)))
    : [csvResources[0]];

  let inserted = 0;
  let skipped = 0;
  let errors = 0;

  for (const resource of targets) {
    console.log(
      `[311] Downloading resource: ${resource.name} (${resource.id}) — ${resource.url}`
    );

    let allRecords: CSVRecord[];
    try {
      allRecords = await downloadAndParseCSV(resource.url);
    } catch (err) {
      console.error(`[311] Failed to download/parse ${resource.name}:`, err);
      errors++;
      continue;
    }

    if (options.limit) allRecords = allRecords.slice(0, options.limit);

    console.log(`[311] Parsed ${allRecords.length} records from ${resource.name}`);

    // Collect unique intersections for batch geocoding
    const uniqueIntersections = new Set<string>();
    for (const r of allRecords) {
      const intersection = buildIntersection(r);
      if (intersection) uniqueIntersections.add(intersection);
    }

    console.log(
      `[311] Batch geocoding ${uniqueIntersections.size} unique intersections...`
    );
    const geocodeMap = await batchGeocode([...uniqueIntersections]);

    // Build category lookup from all unique request types
    const uniqueTypes = [
      ...new Set(
        allRecords.map((r) => field(r, "Service Request Type", "Type") ?? "")
      ),
    ];
    const categoryLookup = buildCategoryLookup(uniqueTypes);

    // Process and upsert in batches
    const rows: ServiceRequestRow[] = [];

    for (const r of allRecords) {
      const createdAt = field(
        r,
        "Service Request Creation Date",
        "Creation Date"
      );

      // Skip records outside our time window for incremental runs
      if (!options.fullLoad && !options.limit && createdAt) {
        const recordDate = new Date(createdAt);
        if (recordDate < since) continue;
      }

      const rawType = field(r, "Service Request Type", "Type") ?? "";
      const intersection = buildIntersection(r);
      const point = intersection ? geocodeMap.get(intersection) : null;

      if (!point) {
        skipped++;
        continue; // Only index geocoded records
      }

      const sourceId =
        field(r, "Service Request ID", "SR Number", "Id") ??
        `${resource.id}:${rowKey(r)}`;

      rows.push({
        source_id: sourceId,
        created_at: createdAt ?? null,
        category: categoryLookup[rawType] ?? normalizeCategory(rawType),
        raw_type: rawType,
        division: field(r, "Division", "Service Request Division") ?? null,
        status: field(r, "Status", "Service Request Status") ?? null,
        ward: field(r, "Ward", "Service Request Ward") ?? null,
        // WKT format for PostGIS — note: PostGIS uses (lng, lat) order
        geom: `SRID=4326;POINT(${point.lng} ${point.lat})`,
      });
    }

    // Upsert in batches
    for (let i = 0; i < rows.length; i += UPSERT_BATCH) {
      const batch = rows.slice(i, i + UPSERT_BATCH);
      const { error } = await supabase
        .from("service_requests")
        .upsert(batch, { onConflict: "source_id", ignoreDuplicates: true });

      if (error) {
        console.error("[311] Upsert error:", error.message);
        errors += batch.length;
      } else {
        inserted += batch.length;
      }
    }
  }

  console.log(
    `[311] Done — inserted: ${inserted}, skipped (no geocode): ${skipped}, errors: ${errors}`
  );
  return { inserted, skipped, errors };
}

// ── Types ─────────────────────────────────────────────────────────────────

type CSVRecord = Record<string, string>;

interface ServiceRequestRow {
  source_id: string;
  created_at: string | null;
  category: string;
  raw_type: string;
  division: string | null;
  status: string | null;
  ward: string | null;
  geom: string; // WKT POINT
}

// ── Helpers ───────────────────────────────────────────────────────────────

/**
 * Download a URL and return all CSV rows as plain objects.
 * Handles both raw CSV files and ZIP archives containing a CSV.
 */
async function downloadAndParseCSV(url: string): Promise<CSVRecord[]> {
  const res = await fetch(url);
  if (!res.ok)
    throw new Error(`HTTP ${res.status} downloading ${url}`);

  const buffer = Buffer.from(await res.arrayBuffer());

  const lowerUrl = url.toLowerCase();
  const contentType = res.headers.get("content-type") ?? "";
  const isZip =
    lowerUrl.endsWith(".zip") ||
    contentType.includes("zip") ||
    contentType.includes("octet-stream");

  let csvBuffer: Buffer;

  if (isZip) {
    const zip = new AdmZip(buffer);
    const csvEntry = zip.getEntries().find((e) =>
      e.entryName.toLowerCase().endsWith(".csv")
    );
    if (!csvEntry)
      throw new Error(`No CSV file found inside ZIP at ${url}`);
    csvBuffer = csvEntry.getData();
  } else {
    csvBuffer = buffer;
  }

  const records = csvParseSync(csvBuffer, {
    columns: true,
    skip_empty_lines: true,
    trim: true,
    relax_column_count: true,
  }) as CSVRecord[];

  return records;
}

/**
 * Read a field from a CSV record by trying multiple known name variants.
 * Falls back to a case-insensitive scan to handle header capitalisation
 * differences between annual files.
 */
function field(record: CSVRecord, ...names: string[]): string | null {
  // Exact match first
  for (const name of names) {
    const val = record[name];
    if (val !== undefined && val !== null && val !== "") {
      return val.trim();
    }
  }
  // Case-insensitive fallback
  const recordKeys = Object.keys(record);
  for (const name of names) {
    const lower = name.toLowerCase();
    const key = recordKeys.find((k) => k.toLowerCase() === lower);
    if (key) {
      const val = record[key];
      if (val !== undefined && val !== null && val !== "") {
        return val.trim();
      }
    }
  }
  return null;
}

/**
 * Combine "Intersection Street 1" and "Intersection Street 2" into a single
 * string suitable for geocodeIntersection. Returns null if both are empty.
 */
function buildIntersection(record: CSVRecord): string | null {
  const street1 = field(record, "Intersection Street 1");
  const street2 = field(record, "Intersection Street 2");
  if (!street1 && !street2) return null;
  if (!street1) return street2;
  if (!street2) return street1;
  return `${street1} and ${street2}`;
}

/**
 * Build a stable key from a record's values for use as a fallback source_id
 * when the dataset doesn't include an explicit ID column.
 */
function rowKey(record: CSVRecord): string {
  const parts = [
    record["Service Request Creation Date"] ?? record["Creation Date"] ?? "",
    buildIntersection(record) ?? "",
    record["Service Request Type"] ?? record["Type"] ?? "",
  ];
  return parts.join("|");
}
