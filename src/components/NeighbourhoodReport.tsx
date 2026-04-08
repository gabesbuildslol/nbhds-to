"use client";

import { useEffect, useState } from "react";
import { ServiceRequests } from "@/components/sections/ServiceRequests";
import { BuildingPermits } from "@/components/sections/BuildingPermits";
import { DineSafe } from "@/components/sections/DineSafe";
import Crime from "@/components/sections/Crime";
import type { NeighbourhoodResponse } from "@/types/neighbourhood";
import type { SummaryResponse } from "@/app/api/summary/route";

interface Props {
  data: NeighbourhoodResponse;
  onUpgrade: () => void;
}

function SummarySkeleton() {
  return (
    <div className="h-4 w-3/4 rounded bg-zinc-100 animate-pulse" />
  );
}

export function NeighbourhoodReport({ data, onUpgrade }: Props) {
  const [summary, setSummary] = useState<SummaryResponse | null>(null);
  const [summaryLoading, setSummaryLoading] = useState(true);

  useEffect(() => {
    setSummaryLoading(true);
    setSummary(null);

    fetch("/api/summary", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(data),
    })
      .then((res) => (res.ok ? res.json() : null))
      .then((json: SummaryResponse | null) => setSummary(json))
      .catch(() => setSummary(null))
      .finally(() => setSummaryLoading(false));
  }, [data]);

  return (
    <div className="max-w-2xl mx-auto pt-8">
      <div className="flex items-baseline gap-3 pb-6 border-b border-zinc-100">
        <h1 className="font-semibold text-xl text-zinc-900">{data.address.formatted}</h1>
        <span className="text-sm text-zinc-400">{data.radius_m}m radius</span>
        {!data.is_paid && (
          <span className="rounded-full px-2 py-0.5 text-xs font-medium text-zinc-500 border border-zinc-200">
            Free preview
          </span>
        )}
      </div>

      {/* AI summary paragraph */}
      <div className="py-5 border-b border-zinc-100">
        {summaryLoading ? (
          <SummarySkeleton />
        ) : summary?.summary ? (
          <p className="text-sm text-zinc-500 leading-relaxed">{summary.summary}</p>
        ) : null}
      </div>

      <div className="flex flex-col gap-0">
        <div className="py-8 border-b border-zinc-100">
          <ServiceRequests
            data={data.service_requests}
            isPaid={data.is_paid}
            onUpgrade={onUpgrade}
            label={summary?.serviceRequestsLabel}
          />
        </div>
        <div className="py-8 border-b border-zinc-100">
          <BuildingPermits
            data={data.building_permits}
            label={summary?.permitsLabel}
          />
        </div>
        <div className="py-8 border-b border-zinc-100">
          <DineSafe
            data={data.dinesafe}
            label={summary?.dineSafeLabel}
          />
        </div>
        <div className="py-8">
          <Crime
            data={data.crime}
            label={summary?.crimeLabel}
          />
        </div>
      </div>

      <div className="pt-6 pb-4 text-center text-xs text-zinc-400">
        Data sourced from{" "}
        <a
          href="https://open.toronto.ca"
          target="_blank"
          rel="noopener noreferrer"
          className="underline underline-offset-2 hover:text-zinc-600 transition-colors"
        >
          Toronto Open Data
        </a>{" "}
        · Updated daily
      </div>
    </div>
  );
}
