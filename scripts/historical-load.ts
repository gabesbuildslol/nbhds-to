import dotenv from "dotenv"
dotenv.config({ path: ".env.local" })

import { ingest311 } from "@/lib/etl/ingest-311"
import { ingestPermits } from "@/lib/etl/ingest-permits"
import { ingestDineSafe } from "@/lib/etl/ingest-dinesafe"
import { ingestCrime } from "@/lib/etl/ingest-crime"
import type { IngestResult } from "@/lib/etl/ingest-311"

interface JobSummary {
  job: string
  inserted: number
  skipped: number
  errors: number
  durationMs: number
}

async function runJob(
  name: string,
  fn: () => Promise<IngestResult>
): Promise<JobSummary> {
  const start = Date.now()
  console.log(`\n${"─".repeat(60)}`)
  console.log(`[historical-load] Starting: ${name}  (${new Date(start).toISOString()})`)
  console.log(`${"─".repeat(60)}`)

  const result = await fn()
  const durationMs = Date.now() - start

  console.log(
    `[historical-load] Finished: ${name} in ${(durationMs / 1000).toFixed(1)}s`
  )

  return { job: name, durationMs, ...result }
}

async function main() {
  console.log("=".repeat(60))
  console.log("[historical-load] Starting full historical load")
  console.log(`[historical-load] Started at: ${new Date().toISOString()}`)
  console.log("=".repeat(60))

  const summaries: JobSummary[] = []

  summaries.push(
    await runJob("311 Service Requests", () =>
      ingest311({ fullLoad: true, since: new Date("2023-01-01") })
    )
  )

  summaries.push(
    await runJob("Building Permits", () => ingestPermits())
  )

  summaries.push(
    await runJob("DineSafe Inspections", () => ingestDineSafe())
  )

  summaries.push(
    await runJob("Neighbourhood Crime Rates", () => ingestCrime())
  )

  // ── Final summary table ──────────────────────────────────────────────────
  const totalMs = summaries.reduce((acc, s) => acc + s.durationMs, 0)

  console.log(`\n${"=".repeat(60)}`)
  console.log("[historical-load] Summary")
  console.log("=".repeat(60))

  const col = (s: string, w: number) => s.padEnd(w).slice(0, w)
  const rCol = (s: string, w: number) => s.padStart(w).slice(-w)

  console.log(
    col("Job", 30) +
      rCol("Inserted", 10) +
      rCol("Skipped", 10) +
      rCol("Errors", 8) +
      rCol("Duration", 12)
  )
  console.log("─".repeat(70))

  for (const s of summaries) {
    console.log(
      col(s.job, 30) +
        rCol(String(s.inserted), 10) +
        rCol(String(s.skipped), 10) +
        rCol(String(s.errors), 8) +
        rCol(`${(s.durationMs / 1000).toFixed(1)}s`, 12)
    )
  }

  console.log("─".repeat(70))
  console.log(
    col("TOTAL", 30) +
      rCol(String(summaries.reduce((a, s) => a + s.inserted, 0)), 10) +
      rCol(String(summaries.reduce((a, s) => a + s.skipped, 0)), 10) +
      rCol(String(summaries.reduce((a, s) => a + s.errors, 0)), 8) +
      rCol(`${(totalMs / 1000).toFixed(1)}s`, 12)
  )
  console.log("=".repeat(60))
}

main().catch((err) => {
  console.error("[historical-load] Fatal error:", err)
  process.exit(1)
})
