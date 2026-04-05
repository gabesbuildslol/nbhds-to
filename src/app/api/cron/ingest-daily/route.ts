/**
 * /app/api/cron/ingest-daily/route.ts
 *
 * Triggered by Vercel Cron at 6:00 AM daily (after CKAN refreshes).
 * Runs all three daily ETL jobs in parallel.
 *
 * vercel.json cron config:
 * {
 *   "crons": [{ "path": "/api/cron/ingest-daily", "schedule": "0 6 * * *" }]
 * }
 *
 * Requires CRON_SECRET env var. Vercel sends this automatically
 * when triggered by the scheduler.
 */

import { NextRequest, NextResponse } from "next/server";
import { ingest311 } from "@/lib/etl/ingest-311";
import { ingestPermits } from "@/lib/etl/ingest-permits";
import { ingestDineSafe } from "@/lib/etl/ingest-dinesafe";

export const dynamic = "force-dynamic";
export const maxDuration = 300; // 5 min — requires Vercel Pro

export async function GET(request: NextRequest) {
  // Vercel cron sends this header automatically
  const authHeader = request.headers.get("authorization");
  if (authHeader !== `Bearer ${process.env.CRON_SECRET}`) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  const startedAt = new Date().toISOString();
  console.log(`[cron] Daily ingest started at ${startedAt}`);

  // Run all three jobs in parallel — they write to separate tables
  const results = await Promise.allSettled([
    ingest311({ since: new Date(Date.now() - 7 * 86400000) }),
    ingestPermits(),
    ingestDineSafe(),
  ]);

  const jobs = ["311", "permits", "dinesafe"] as const;
  const summary = results.map((r, i) => ({
    job: jobs[i],
    status: r.status,
    ...(r.status === "fulfilled"
      ? r.value
      : { error: (r.reason as Error)?.message ?? "Unknown error" }),
  }));

  const allSucceeded = results.every((r) => r.status === "fulfilled");
  console.log("[cron] Results:", JSON.stringify(summary));

  return NextResponse.json(
    { started_at: startedAt, success: allSucceeded, jobs: summary },
    { status: allSucceeded ? 200 : 207 }
  );
}
