"use client";

import type { CrimeSummary } from "@/types/neighbourhood";

const CITY_AVG_ASSAULT_RATE = 180; // ~180 per 100k city-wide

interface CrimeProps {
  data: CrimeSummary | null;
  label?: string;
}

function TrendIndicator({ pct }: { pct: number }) {
  const arrow = pct > 0 ? "↑" : "↓";
  return (
    <span className="text-xs text-zinc-400">
      {arrow} {Math.abs(pct)}% vs last year
    </span>
  );
}

export default function Crime({ data, label }: CrimeProps) {
  if (!data) {
    return (
      <div>
        <h2 className="text-xs font-semibold uppercase tracking-widest text-zinc-400">
          Crime rates
        </h2>
        <p className="mt-4 text-sm text-zinc-400 italic">
          Crime rate data not available for this location
        </p>
      </div>
    );
  }

  const stats = [
    { label: "Assault", value: data.rates.assault, isAssault: true },
    { label: "Auto theft", value: data.rates.auto_theft, isAssault: false },
    { label: "Break & enter", value: data.rates.break_enter, isAssault: false },
    { label: "Robbery", value: data.rates.robbery, isAssault: false },
  ];

  return (
    <div>
      <h2 className="text-xs font-semibold uppercase tracking-widest text-zinc-400">
        Crime rates
      </h2>
      {label && (
        <p className="text-sm text-zinc-400 mt-1 italic">{label}</p>
      )}
      <p className="text-xs text-zinc-400 mt-1">
        Rates per 100,000 population · {data.year} · Source: Toronto Police
        Service via Toronto Open Data
      </p>
      <div className="grid grid-cols-2 gap-6 mt-4">
        {stats.map(({ label: statLabel, value, isAssault }) => (
          <div key={statLabel} className="py-2">
            <p className="text-3xl font-semibold text-zinc-900">
              {value !== null ? Math.round(value).toLocaleString() : "—"}
            </p>
            <p className="text-xs text-zinc-400 mt-1">{statLabel}</p>
            {isAssault && (
              <div className="flex flex-col gap-0.5 mt-1">
                <span className="text-xs text-zinc-400">
                  city avg ~{CITY_AVG_ASSAULT_RATE}
                  {value !== null && value !== CITY_AVG_ASSAULT_RATE && (
                    <> · {value < CITY_AVG_ASSAULT_RATE ? "below" : "above"} avg</>
                  )}
                </span>
                {data.assault_trend_pct !== null && (
                  <TrendIndicator pct={data.assault_trend_pct} />
                )}
              </div>
            )}
          </div>
        ))}
      </div>
    </div>
  );
}
