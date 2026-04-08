"use client";

import { BuildingPermitSummary, RecentPermit } from "@/types/neighbourhood";

function formatDate(dateStr: string | null): string {
  if (!dateStr) return "—";
  const d = new Date(dateStr);
  return d.toLocaleDateString("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
  });
}

function roundToNearest10(n: number): number {
  return Math.round(n / 10) * 10;
}

function isActiveStatus(status: string): boolean {
  const s = status.toLowerCase();
  return s === "open" || s === "issued" || s === "active";
}

function PermitBadge({ status }: { status: string }) {
  if (isActiveStatus(status)) {
    return (
      <span className="inline-flex items-center rounded-full px-2 py-0.5 text-xs font-medium bg-amber-50 text-amber-700">
        {status}
      </span>
    );
  }
  return (
    <span className="inline-flex items-center rounded-full px-2 py-0.5 text-xs font-medium text-zinc-500 border border-zinc-200">
      {status}
    </span>
  );
}

function PermitRow({ permit }: { permit: RecentPermit }) {
  const description = permit.description
    ? permit.description.length > 100
      ? permit.description.slice(0, 100) + "…"
      : permit.description
    : null;

  return (
    <li className="flex flex-col gap-1 py-3 border-b border-zinc-100 last:border-b-0">
      <div className="flex items-start justify-between gap-2">
        <span className="font-medium text-sm text-zinc-800 leading-snug">{permit.permit_type}</span>
        <PermitBadge status={permit.status} />
      </div>
      {description && (
        <p className="text-sm text-zinc-500 leading-snug">{description}</p>
      )}
      <div className="flex items-center gap-3 text-xs text-zinc-400">
        <span>{formatDate(permit.issued_date)}</span>
        <span>{roundToNearest10(permit.distance_m)} m away</span>
      </div>
    </li>
  );
}

const CITY_AVG_PERMITS = 8; // ~8 active permits per 500m radius

interface BuildingPermitsProps {
  data: BuildingPermitSummary;
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

export function BuildingPermits({ data, label }: BuildingPermitsProps) {
  return (
    <div>
      <h2 className="text-xs font-semibold uppercase tracking-widest text-zinc-400">
        Building permits
      </h2>
      {label && (
        <p className="text-sm text-zinc-400 mt-1 italic">{label}</p>
      )}
      <div className="mt-4">
        {data.total_count === 0 ? (
          <p className="text-sm text-zinc-400 italic">
            No building permits found near this address
          </p>
        ) : (
          <>
            <div>
              {data.active_count > 0 ? (
                <div className="flex items-baseline gap-3 flex-wrap">
                  <p className="text-2xl font-semibold text-zinc-800">
                    {data.active_count}{" "}
                    <span className="text-base font-normal text-zinc-500">
                      active permit{data.active_count !== 1 ? "s" : ""} within 500m
                    </span>
                  </p>
                  <span className="text-xs text-zinc-400">city avg ~{CITY_AVG_PERMITS}</span>
                  {data.trend_pct !== null && <TrendIndicator pct={data.trend_pct} />}
                </div>
              ) : (
                <p className="text-sm text-zinc-400 italic">No active permits nearby</p>
              )}
            </div>
            {data.recent.length > 0 && (
              <ul className="mt-4">
                {data.recent.map((permit, i) => (
                  <PermitRow key={i} permit={permit} />
                ))}
              </ul>
            )}
          </>
        )}
      </div>
    </div>
  );
}
