/**
 * POST /api/summary
 *
 * Accepts a NeighbourhoodResponse body and returns a Claude-generated
 * plain-English narrative for home buyers — a top-level summary plus one
 * interpretive label per data section.
 */

import { NextRequest, NextResponse } from "next/server";
import type { NeighbourhoodResponse } from "@/types/neighbourhood";

export interface SummaryResponse {
  summary: string;
  serviceRequestsLabel: string;
  permitsLabel: string;
  dineSafeLabel: string;
  crimeLabel: string;
}

function buildPrompt(data: NeighbourhoodResponse): string {
  const sr = data.service_requests;
  const bp = data.building_permits;
  const ds = data.dinesafe;
  const crime = data.crime;

  const topCategories = sr.by_category
    .slice(0, 3)
    .map((c) => `${c.label} (${c.count})`)
    .join(", ");

  const srTrend =
    sr.trend_pct !== null
      ? `${sr.trend_pct > 0 ? "+" : ""}${sr.trend_pct}% vs prior year`
      : "trend unavailable";

  const permitTrend =
    bp.trend_pct !== null
      ? `${bp.trend_pct > 0 ? "+" : ""}${bp.trend_pct}% vs prior year`
      : "trend unavailable";

  const dineTrend =
    ds.pass_rate_trend_pp !== null
      ? `${ds.pass_rate_trend_pp > 0 ? "+" : ""}${ds.pass_rate_trend_pp}pp vs prior year`
      : "trend unavailable";

  const crimeAssault = crime?.rates.assault ?? null;
  const crimeTrend =
    crime?.assault_trend_pct !== null && crime?.assault_trend_pct !== undefined
      ? `${crime.assault_trend_pct > 0 ? "+" : ""}${crime.assault_trend_pct}% vs prior year`
      : "trend unavailable";

  return `You are a real estate data analyst writing for home buyers who want clear, neutral, factual summaries. Based on the following neighbourhood data, respond with a JSON object containing exactly these five string fields. Respond with a raw JSON object only. Do not use markdown. Do not wrap in backticks. Do not include any text before or after the JSON object.

Fields required:
- summary: 2-3 sentences synthesizing all four data sections. Be specific with numbers. Example: "This block shows moderate 311 activity driven mostly by road maintenance requests, active renovation permits suggesting investment in the area, a strong restaurant inspection record, and crime rates below the city average."
- serviceRequestsLabel: one-line interpretation of 311 activity. Example: "Mostly road and noise complaints — typical for this area"
- permitsLabel: one-line interpretation of building permits. Example: "Active renovation zone — 12 permits in progress"
- dineSafeLabel: one-line interpretation of restaurant inspections. Example: "Strong inspection record — 94% pass rate nearby"
- crimeLabel: one-line interpretation of crime rates. Example: "Below city average across most categories"

Neighbourhood data:
311 Service Requests: ${sr.total} reports in the last 90 days. Top categories: ${topCategories || "none"}. Year-over-year: ${srTrend}. City average: ~45 per 500m radius per 90 days.

Building Permits: ${bp.active_count} active permit${bp.active_count !== 1 ? "s" : ""} within 500m (${bp.total_count} total). Year-over-year active count: ${permitTrend}. City average: ~8 active permits per 500m radius.

Restaurant Inspections (DineSafe): ${ds.pass_rate !== null ? `${ds.pass_rate}% pass rate` : "pass rate unavailable"} across ${ds.total_establishments} establishment${ds.total_establishments !== 1 ? "s" : ""}. ${ds.failed_last_90d > 0 ? `${ds.failed_last_90d} conditional pass or closure in the last 90 days.` : "No failures in the last 90 days."} Year-over-year pass rate change: ${dineTrend}. City average: ~87% pass rate.

Crime Rates (${crime ? `${crime.neighbourhood}, ${crime.year}` : "unavailable"}): Assault rate ${crimeAssault !== null ? `${Math.round(crimeAssault).toLocaleString()} per 100k` : "unavailable"}. Year-over-year assault change: ${crimeTrend}. City average assault rate: ~180 per 100k.`;
}

export async function POST(request: NextRequest) {
  let data: NeighbourhoodResponse;

  try {
    data = await request.json();
  } catch {
    return NextResponse.json({ error: "Invalid JSON body" }, { status: 400 });
  }

  if (!data?.address || !data?.service_requests) {
    return NextResponse.json({ error: "Invalid neighbourhood data" }, { status: 400 });
  }

  const prompt = buildPrompt(data);

  const anthropicRes = await fetch("https://api.anthropic.com/v1/messages", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "x-api-key": process.env.ANTHROPIC_API_KEY!,
      "anthropic-version": "2023-06-01",
    },
    body: JSON.stringify({
      model: "claude-haiku-4-5-20251001",
      max_tokens: 500,
      messages: [{ role: "user", content: prompt }],
    }),
  });

  if (!anthropicRes.ok) {
    const errorText = await anthropicRes.text();
    console.error("[api/summary] Anthropic error:", errorText);
    return NextResponse.json({ error: "Summary generation failed" }, { status: 502 });
  }

  const anthropicData = await anthropicRes.json();
  const responseText: string = anthropicData?.content?.[0]?.text ?? "";

  const raw = responseText
    .replace(/^```json\s*/i, "")
    .replace(/^```\s*/i, "")
    .replace(/```\s*$/i, "")
    .trim();

  let summary: SummaryResponse;
  try {
    summary = JSON.parse(raw);
  } catch {
    console.error("[api/summary] Failed to parse Claude JSON:", raw);
    return NextResponse.json({ error: "Invalid summary format" }, { status: 502 });
  }

  return NextResponse.json(summary, {
    headers: {
      // Cache per address for 1 hour — the underlying data doesn't change faster
      "Cache-Control": "public, s-maxage=3600, stale-while-revalidate=7200",
    },
  });
}
