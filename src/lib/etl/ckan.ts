const CKAN_BASE =
  "https://ckan0.cf.opendata.inter.prod-toronto.ca/api/3/action";

export interface CKANResource {
  id: string;
  name: string;
  url: string;
  format: string;
  datastore_active: boolean;
  last_modified: string;
}

export interface CKANRecord {
  _id: number;
  [key: string]: unknown;
}

export interface DatastoreResult {
  records: CKANRecord[];
  total: number;
  fields: Array<{ id: string; type: string }>;
}

/** Fetch all resources for a package (dataset). Use this to get resource IDs. */
export async function getPackageResources(
  packageId: string
): Promise<CKANResource[]> {
  const res = await fetch(`${CKAN_BASE}/package_show?id=${packageId}`);
  if (!res.ok) throw new Error(`CKAN package_show failed: ${res.status}`);
  const data = await res.json();
  if (!data.success) throw new Error(`CKAN error: ${data.error?.message}`);
  return data.result.resources;
}

/** Single page fetch from a CKAN datastore resource. */
export async function datastoreSearch(
  resourceId: string,
  {
    limit = 1000,
    offset = 0,
    filters,
  }: {
    limit?: number;
    offset?: number;
    filters?: Record<string, string>;
  } = {}
): Promise<DatastoreResult> {
  const params = new URLSearchParams({
    id: resourceId,
    limit: String(limit),
    offset: String(offset),
  });
  if (filters) params.set("filters", JSON.stringify(filters));

  const res = await fetch(`${CKAN_BASE}/datastore_search?${params}`);
  if (!res.ok) throw new Error(`CKAN datastore_search failed: ${res.status}`);
  const data = await res.json();
  if (!data.success) throw new Error(`CKAN error: ${data.error?.message}`);
  return data.result;
}

/**
 * Async generator that paginates through ALL records in a CKAN resource.
 * Yields batches of records — pipe directly into your ingestion logic.
 *
 * @example
 * for await (const batch of paginateCKAN(resourceId)) {
 *   await processBatch(batch)
 * }
 */
export async function* paginateCKAN(
  resourceId: string,
  pageSize = 5000
): AsyncGenerator<CKANRecord[]> {
  let offset = 0;

  while (true) {
    const result = await datastoreSearch(resourceId, {
      limit: pageSize,
      offset,
    });

    if (!result.records.length) break;
    yield result.records;
    if (result.records.length < pageSize) break; // last page
    offset += pageSize;

    // Respect CKAN — small delay between pages
    await new Promise((r) => setTimeout(r, 200));
  }
}

/**
 * Get the most recently updated datastore-active resource from a package.
 * Useful for datasets that publish annual files (e.g. 311 data by year).
 */
export function getMostRecentResource(
  resources: CKANResource[]
): CKANResource | null {
  const active = resources.filter((r) => r.datastore_active);
  if (!active.length) return null;
  return active.sort(
    (a, b) =>
      new Date(b.last_modified).getTime() - new Date(a.last_modified).getTime()
  )[0];
}
