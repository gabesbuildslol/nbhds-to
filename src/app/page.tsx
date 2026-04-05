"use client";

import { useRef, useState } from "react";
import AddressSearch from "@/components/AddressSearch";
import { NeighbourhoodReport } from "@/components/NeighbourhoodReport";
import type { NeighbourhoodResponse } from "@/types/neighbourhood";

export default function Home() {
  const [reportData, setReportData] = useState<NeighbourhoodResponse | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [currentAddress, setCurrentAddress] = useState("");
  const reportRef = useRef<HTMLDivElement>(null);

  async function handleUpgrade() {
    const response = await fetch("/api/checkout", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ address: currentAddress }),
    });
    const { url } = await response.json();
    window.location.href = url;
  }

  return (
    <div className="flex flex-col font-sans bg-white min-h-screen">
      {/* Nav */}
      <nav className="sticky top-0 z-10 bg-white/80 backdrop-blur-sm border-b border-zinc-100 px-6 flex items-center h-14">
        <span className="font-semibold text-base tracking-tight text-zinc-900">nbhds.to</span>
      </nav>

      {/* Hero */}
      <section
        className="relative pt-24 pb-16 flex flex-col items-center px-4"
      >
        <h1 className="font-sans font-semibold text-3xl tracking-tight text-center max-w-xl text-zinc-900 leading-tight">
          Know what&apos;s actually happening around any Toronto address
        </h1>
        <p className="text-base text-center max-w-lg mt-3 text-zinc-500">
          311 complaints, building permits, restaurant inspections, and crime
          rates — in one place.
        </p>

        <div className="max-w-2xl w-full mt-8">
          <AddressSearch
            onResult={(data) => {
              setReportData(data);
              setCurrentAddress(data.address.formatted);
              setTimeout(() => {
                reportRef.current?.scrollIntoView({ behavior: "smooth", block: "start" });
              }, 100);
            }}
            onLoading={setIsLoading}
          />
        </div>

        {isLoading && reportData === null && (
          <div className="mt-8 flex items-center justify-center">
            <div className="h-8 w-8 animate-spin rounded-full border-2 border-zinc-900 border-t-transparent" />
          </div>
        )}
      </section>

      {/* Report */}
      {reportData !== null && (
        <section ref={reportRef} className="transition-opacity duration-500 opacity-100 px-4 pb-16">
          <NeighbourhoodReport data={reportData} onUpgrade={handleUpgrade} />
        </section>
      )}

      {/* Footer */}
      <footer className="mt-auto border-t border-zinc-100 py-6 text-center text-sm text-zinc-400">
        nbhds.to · Data from Toronto Open Data · Not affiliated with the City of
        Toronto
      </footer>
    </div>
  );
}
