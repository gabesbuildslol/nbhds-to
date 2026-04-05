"use client";

import type { CrimeSummary } from "@/types/neighbourhood";

interface CrimeProps {
  data: CrimeSummary | null;
}

export default function Crime({ data }: CrimeProps) {
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
    { label: "Assault", value: data.rates.assault },
    { label: "Auto theft", value: data.rates.auto_theft },
    { label: "Break & enter", value: data.rates.break_enter },
    { label: "Robbery", value: data.rates.robbery },
  ];

  return (
    <div>
      <h2 className="text-xs font-semibold uppercase tracking-widest text-zinc-400">
        Crime rates
      </h2>
      <p className="text-xs text-zinc-400 mt-1">
        Rates per 100,000 population · {data.year} · Source: Toronto Police
        Service via Toronto Open Data
      </p>
      <div className="grid grid-cols-2 gap-6 mt-4">
        {stats.map(({ label, value }) => (
          <div key={label} className="py-2">
            <p className="text-3xl font-semibold text-zinc-900">
              {value !== null ? value.toLocaleString() : "—"}
            </p>
            <p className="text-xs text-zinc-400 mt-1">{label}</p>
          </div>
        ))}
      </div>
    </div>
  );
}
