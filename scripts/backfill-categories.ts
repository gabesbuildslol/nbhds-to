import dotenv from "dotenv"
dotenv.config({ path: ".env.local" })

import { createClient } from "@supabase/supabase-js"
import { normalizeCategory } from "@/lib/etl/normalizer"

const supabase = createClient(
  process.env.SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_KEY!
)

const FETCH_SIZE = 1000
// PostgREST encodes each UUID in the IN filter URL; keep well under the limit
const UPDATE_CHUNK = 200

function chunk<T>(arr: T[], size: number): T[][] {
  const out: T[][] = []
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size))
  return out
}

async function main() {
  console.log("=".repeat(60))
  console.log("[backfill-categories] Starting category backfill")
  console.log(`[backfill-categories] Started at: ${new Date().toISOString()}`)
  console.log("=".repeat(60))

  let totalFetched = 0
  let totalUpdated = 0
  let totalErrors = 0

  while (true) {
    // Always fetch from offset 0 — updated rows leave the "other" pool
    // so the next batch always contains unprocessed rows.
    const { data: rows, error: fetchError } = await supabase
      .from("service_requests")
      .select("id, raw_type")
      .eq("category", "other")
      .limit(FETCH_SIZE)

    if (fetchError) {
      console.error("[backfill-categories] Fetch error:", fetchError.message)
      process.exit(1)
    }

    if (!rows || rows.length === 0) break

    totalFetched += rows.length

    // Partition rows that can be remapped away from "other"
    const toUpdate = rows
      .map((row) => ({
        id: row.id as string,
        category: normalizeCategory(row.raw_type ?? ""),
      }))
      .filter((r) => r.category !== "other")

    // If nothing in this batch can be remapped, we've hit the permanent floor
    if (toUpdate.length === 0) {
      console.log(
        `[backfill-categories] Remaining ${rows.length.toLocaleString()} rows cannot be remapped — stopping.`
      )
      break
    }

    // Group by new category, then update in safe-sized ID chunks
    const byCategory = new Map<string, string[]>()
    for (const { id, category } of toUpdate) {
      const ids = byCategory.get(category) ?? []
      ids.push(id)
      byCategory.set(category, ids)
    }

    for (const [category, ids] of byCategory) {
      for (const idChunk of chunk(ids, UPDATE_CHUNK)) {
        const { error: updateError } = await supabase
          .from("service_requests")
          .update({ category })
          .in("id", idChunk)

        if (updateError) {
          console.error(
            `[backfill-categories] Update error (category=${category}):`,
            updateError.message
          )
          totalErrors += idChunk.length
        } else {
          totalUpdated += idChunk.length
        }
      }
    }

    console.log(
      `[backfill-categories] Fetched ${totalFetched.toLocaleString()} rows total` +
        ` | Updated so far: ${totalUpdated.toLocaleString()}` +
        ` | Errors: ${totalErrors.toLocaleString()}`
    )
  }

  console.log(`\n${"=".repeat(60)}`)
  console.log("[backfill-categories] Done")
  console.log(`  Rows fetched:  ${totalFetched.toLocaleString()}`)
  console.log(`  Rows updated:  ${totalUpdated.toLocaleString()}`)
  console.log(`  Rows errored:  ${totalErrors.toLocaleString()}`)
  console.log("=".repeat(60))
}

main().catch((err) => {
  console.error("[backfill-categories] Fatal error:", err)
  process.exit(1)
})
