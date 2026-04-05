"use client";

import { useEffect, useRef, useState } from "react";
import type { NeighbourhoodResponse } from "@/types/neighbourhood";

declare global {
  interface Window {
    google: typeof google;
    initGoogleMaps?: () => void;
  }
}

interface AddressSearchProps {
  onResult: (data: NeighbourhoodResponse) => void;
  onLoading: (loading: boolean) => void;
}

const MAPS_API_KEY = process.env.NEXT_PUBLIC_GOOGLE_MAPS_API_KEY ?? "";
const MAPS_SCRIPT_ID = "google-maps-script";

function loadGoogleMapsScript(): Promise<void> {
  return new Promise((resolve, reject) => {
    if (typeof window === "undefined") return;

    if (window.google?.maps?.places) {
      resolve();
      return;
    }

    if (document.getElementById(MAPS_SCRIPT_ID)) {
      // Script tag already injected — wait for the callback
      window.initGoogleMaps = resolve;
      return;
    }

    window.initGoogleMaps = resolve;

    const script = document.createElement("script");
    script.id = MAPS_SCRIPT_ID;
    script.src = `https://maps.googleapis.com/maps/api/js?key=${MAPS_API_KEY}&libraries=places&callback=initGoogleMaps`;
    script.async = true;
    script.defer = true;
    script.onerror = () => reject(new Error("Failed to load Google Maps API"));
    document.head.appendChild(script);
  });
}

export default function AddressSearch({ onResult, onLoading }: AddressSearchProps) {
  const inputRef = useRef<HTMLInputElement>(null);
  const autocompleteRef = useRef<google.maps.places.Autocomplete | null>(null);
  const selectedAddressRef = useRef<string>("");

  const [inputValue, setInputValue] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;

    loadGoogleMapsScript()
      .then(() => {
        if (cancelled || !inputRef.current) return;

        const autocomplete = new window.google.maps.places.Autocomplete(inputRef.current, {
          types: ["address"],
          componentRestrictions: { country: "CA" },
          bounds: new window.google.maps.LatLngBounds(
            { lat: 43.5810, lng: -79.6393 }, // SW corner of Toronto
            { lat: 43.8555, lng: -79.1152 }  // NE corner of Toronto
          ),
          strictBounds: false,
          fields: ["formatted_address"],
        });

        autocomplete.addListener("place_changed", () => {
          const place = autocomplete.getPlace();
          const address = place.formatted_address ?? inputRef.current?.value ?? "";
          selectedAddressRef.current = address;
          setInputValue(address);
        });

        autocompleteRef.current = autocomplete;
      })
      .catch(() => {
        if (!cancelled) setError("Could not load address suggestions.");
      });

    return () => {
      cancelled = true;
    };
  }, []);

  async function fetchNeighbourhood(address: string) {
    if (!address.trim()) return;

    setError(null);
    setLoading(true);
    onLoading(true);

    try {
      const res = await fetch(
        `/api/neighbourhood?address=${encodeURIComponent(address)}&radius=500`
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

  function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    const address = selectedAddressRef.current || inputValue;
    fetchNeighbourhood(address);
  }

  return (
    <form onSubmit={handleSubmit} className="w-full">
      <div className="flex w-full gap-2">
        <input
          ref={inputRef}
          type="text"
          value={inputValue}
          onChange={(e) => {
            setInputValue(e.target.value);
            selectedAddressRef.current = "";
          }}
          placeholder="Enter a Toronto address…"
          disabled={loading}
          className="
            flex-1 rounded-lg border border-gray-300 bg-white px-4 py-3
            font-sans text-base text-gray-900 placeholder-gray-400
            shadow-sm outline-none transition
            focus:border-gray-900 focus:ring-2 focus:ring-gray-900/10
            disabled:cursor-not-allowed disabled:opacity-50
          "
        />
        <button
          type="submit"
          disabled={loading || !inputValue.trim()}
          className="
            rounded-lg bg-gray-900 px-6 py-3 font-sans text-base
            font-medium text-white shadow-sm transition
            hover:bg-gray-700 active:bg-gray-800
            disabled:cursor-not-allowed disabled:opacity-50
          "
        >
          {loading ? "Searching…" : "Search"}
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
