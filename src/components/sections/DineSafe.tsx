"use client";

import type { DineSafeSummary, DineSafeResult } from "@/types/neighbourhood";

const CITY_AVG_DINESAFE_PASS_RATE = 87; // ~87% pass rate city-wide

interface Props {
  data: DineSafeSummary;
  label?: string;
}

function passRateColor(rate: number): string {
  if (rate >= 90) return "text-green-600";
  if (rate >= 75) return "text-amber-600";
  return "text-red-600";
}

function formatDate(iso: string): string {
  return new Date(iso).toLocaleDateString("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
  });
}

function roundToNearest10(n: number): number {
  return Math.round(n / 10) * 10;
}

function ResultBadge({ result }: { result: DineSafeResult }) {
  if (result === "Pass") {
    return (
      <span className="inline-flex items-center rounded-full px-2 py-0.5 text-xs font-medium text-green-600 border border-green-200">
        Pass
      </span>
    );
  }
  if (result === "Conditional Pass") {
    return (
      <span className="inline-flex items-center rounded-full px-2 py-0.5 text-xs font-medium text-amber-600 border border-amber-200">
        Conditional Pass
      </span>
    );
  }
  return (
    <span className="inline-flex items-center rounded-full px-2 py-0.5 text-xs font-medium bg-red-50 text-red-600">
      Closed
    </span>
  );
}

function TrendIndicator({ pp }: { pp: number }) {
  const arrow = pp > 0 ? "↑" : "↓";
  return (
    <span className="text-xs text-zinc-400">
      {arrow} {Math.abs(pp)}pp vs last year
    </span>
  );
}

export function DineSafe({ data, label }: Props) {
  const vsAvg =
    data.pass_rate !== null ? Math.round(data.pass_rate - CITY_AVG_DINESAFE_PASS_RATE) : null;

  return (
    <div>
      <h2 className="text-xs font-semibold uppercase tracking-widest text-zinc-400">
        Restaurant inspections
      </h2>
      {label && (
        <p className="text-sm text-zinc-400 mt-1 italic">{label}</p>
      )}

      {data.total_establishments === 0 ? (
        <p className="mt-4 text-sm text-zinc-400 italic">
          No food establishments found near this address
        </p>
      ) : (
        <div className="mt-4 space-y-4">
          <div className="flex gap-8 items-end flex-wrap">
            <div>
              <span
                className={`text-2xl font-semibold ${
                  data.pass_rate !== null
                    ? passRateColor(data.pass_rate)
                    : "text-zinc-400"
                }`}
              >
                {data.pass_rate !== null ? `${data.pass_rate}%` : "—"}
              </span>
              <p className="text-sm text-zinc-400">pass rate</p>
            </div>
            <div>
              <span className="text-2xl font-semibold text-zinc-800">
                {data.total_establishments}
              </span>
              <p className="text-sm text-zinc-400">
                establishments nearby
              </p>
            </div>
            <div className="flex flex-col gap-0.5 pb-0.5">
              <span className="text-xs text-zinc-400">city avg ~{CITY_AVG_DINESAFE_PASS_RATE}%{vsAvg !== null && vsAvg !== 0 && (
                <> · {vsAvg > 0 ? "+" : ""}{vsAvg}pp vs avg</>
              )}</span>
              {data.pass_rate_trend_pp !== null && (
                <TrendIndicator pp={data.pass_rate_trend_pp} />
              )}
            </div>
          </div>

          {data.failed_last_90d > 0 && (
            <div className="rounded-md bg-amber-50 px-3 py-2 text-sm text-amber-700">
              {data.failed_last_90d} conditional pass or closure in the last 90
              days
            </div>
          )}

          <ul className="divide-y divide-zinc-100">
            {data.recent_inspections.map((inspection, i) => (
              <li
                key={i}
                className="flex items-start justify-between gap-4 py-3"
              >
                <div className="min-w-0">
                  <p className="truncate text-sm font-medium text-zinc-800">
                    {inspection.establishment}
                  </p>
                  {inspection.estab_type && (
                    <p className="text-xs text-zinc-400">
                      {inspection.estab_type}
                    </p>
                  )}
                </div>
                <div className="flex shrink-0 flex-col items-end gap-1">
                  <ResultBadge result={inspection.result} />
                  <span className="text-xs text-zinc-400">
                    {formatDate(inspection.inspection_date)}
                  </span>
                  <span className="text-xs text-zinc-400">
                    {roundToNearest10(inspection.distance_m)} m
                  </span>
                </div>
              </li>
            ))}
          </ul>
        </div>
      )}
    </div>
  );
}
