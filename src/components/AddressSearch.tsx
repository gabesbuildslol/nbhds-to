"use client";

import { useState } from "react";
import { ChevronRight } from "lucide-react";
import type { NeighbourhoodResponse } from "@/types/neighbourhood";

interface AddressSearchProps {
  onResult: (data: NeighbourhoodResponse) => void;
  onLoading: (loading: boolean) => void;
}

export default function AddressSearch({ onResult, onLoading }: AddressSearchProps) {
  const [inputValue, setInputValue] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    if (!inputValue.trim()) return;

    setError(null);
    setLoading(true);
    onLoading(true);

    try {
      const res = await fetch(
        `/api/neighbourhood?address=${encodeURIComponent(inputValue)}&radius=500`
      );

      if (!res.ok) {
        const body = await res.json().catch(() => ({}));
        throw new Error((body as { error?: string }).error ?? `Request failed (${res.status})`);
      }

      const data: NeighbourhoodResponse = await res.json();
      onResult(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Something went wrong. Please try again.");
    } finally {
      setLoading(false);
      onLoading(false);
    }
  }

  return (
    <form onSubmit={handleSubmit} className="w-full">
      <div className="relative w-full">
        <input
          type="text"
          value={inputValue}
          onChange={(e) => setInputValue(e.target.value)}
          placeholder="Enter an address..."
          disabled={loading}
          className="
            h-12 w-full rounded-full border border-zinc-200 bg-white
            pl-5 pr-12 text-sm text-zinc-900 outline-none
            transition focus:shadow-md
            disabled:cursor-not-allowed disabled:opacity-50
          "
        />
        <button
          type="submit"
          disabled={loading}
          className="
            absolute right-1.5 top-1/2 -translate-y-1/2
            flex items-center justify-center
            h-9 w-9 rounded-full bg-zinc-900 text-white
            transition hover:bg-zinc-700 active:bg-zinc-800
            disabled:cursor-not-allowed disabled:opacity-40
          "
        >
          {loading ? (
            <div className="h-4 w-4 animate-spin rounded-full border-2 border-white border-t-transparent" />
          ) : (
            <ChevronRight className="h-4 w-4" />
          )}
        </button>
      </div>

      {error && (
        <p className="mt-2 font-sans text-sm text-red-600" role="alert">
          {error}
        </p>
      )}
    </form>
  );
}
