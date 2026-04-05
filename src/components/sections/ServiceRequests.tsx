"use client";

import type {
  ServiceRequestSummary,
  RecentServiceRequest,
  ServiceRequestStatus,
} from "@/types/neighbourhood";

interface Props {
  data: ServiceRequestSummary;
  isPaid: boolean;
  onUpgrade: () => void;
}

function relativeDate(isoString: string): string {
  const now = Date.now();
  const then = new Date(isoString).getTime();
  const diffMs = now - then;
  const diffDays = Math.floor(diffMs / (1000 * 60 * 60 * 24));

  if (diffDays === 0) return "Today";
  if (diffDays === 1) return "Yesterday";
  if (diffDays < 7) return `${diffDays} days ago`;
  if (diffDays < 30) {
    const weeks = Math.floor(diffDays / 7);
    return `${weeks} ${weeks === 1 ? "week" : "weeks"} ago`;
  }
  const months = Math.floor(diffDays / 30);
  return `${months} ${months === 1 ? "month" : "months"} ago`;
}

function roundToNearest10(n: number): number {
  return Math.round(n / 10) * 10;
}

function StatusBadge({ status }: { status: ServiceRequestStatus }) {
  const isOpen = status !== "Closed" && status !== "Cancelled";
  return (
    <span
      className={`inline-flex items-center rounded-full px-2 py-0.5 text-xs font-medium ${
        isOpen
          ? "bg-amber-50 text-amber-700"
          : "text-zinc-500 border border-zinc-200"
      }`}
    >
      {status}
    </span>
  );
}

function CategoryBar({
  label,
  count,
  max,
}: {
  label: string;
  count: number;
  max: number;
}) {
  const pct = max > 0 ? (count / max) * 100 : 0;

  return (
    <div className="flex items-center gap-3">
      <span className="w-36 shrink-0 truncate text-sm text-zinc-800">{label}</span>
      <div className="flex-1 h-1.5 rounded-full bg-zinc-100 overflow-hidden">
        <div
          className="h-full rounded-full bg-zinc-900 transition-all"
          style={{ width: `${pct}%` }}
        />
      </div>
      <span className="w-8 shrink-0 text-right text-sm text-zinc-400">
        {count}
      </span>
    </div>
  );
}

function RecentRequestRow({ req }: { req: RecentServiceRequest }) {
  return (
    <div className="flex items-start justify-between gap-3 py-2">
      <div className="flex flex-col gap-0.5 min-w-0">
        <span className="text-sm font-medium text-zinc-800 truncate">{req.label}</span>
        <span className="text-xs text-zinc-400">
          {relativeDate(req.created_at)} &middot;{" "}
          {roundToNearest10(req.distance_m)} m away
        </span>
      </div>
      <StatusBadge status={req.status} />
    </div>
  );
}

export function ServiceRequests({ data, isPaid, onUpgrade }: Props) {
  const maxCount = data.by_category.reduce(
    (acc, cat) => Math.max(acc, cat.count),
    0
  );
  const recent = data.recent.slice(0, 5);

  return (
    <div>
      <h2 className="text-xs font-semibold uppercase tracking-widest text-zinc-400">
        Service requests
      </h2>
      <p className="text-sm text-zinc-500 mt-1">
        {data.total} reports in the last 90 days
      </p>

      <div className="mt-4 space-y-6">
        {data.by_category.length > 0 && (
          <div className="space-y-2">
            {data.by_category.map((cat) => (
              <CategoryBar
                key={cat.category}
                label={cat.label}
                count={cat.count}
                max={maxCount}
              />
            ))}
          </div>
        )}

        {recent.length > 0 && (
          <div className="divide-y divide-zinc-100">
            {recent.map((req, i) => (
              <RecentRequestRow key={i} req={req} />
            ))}
          </div>
        )}

        {!isPaid && (
          <div className="flex flex-col gap-3 pt-4 border-t border-zinc-100">
            <p className="text-sm text-zinc-400">
              Showing last 90 days
            </p>
            <button
              onClick={onUpgrade}
              className="self-start rounded-full border border-zinc-200 px-4 py-2 text-sm font-medium text-zinc-700 transition hover:bg-zinc-50 active:bg-zinc-100"
            >
              Unlock full history — $18
            </button>
          </div>
        )}
      </div>
    </div>
  );
}
