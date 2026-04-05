async function main() {
  const { createClient } = await import('@supabase/supabase-js');
  const { getPackageResources, paginateCKAN } = await import('./src/lib/etl/ckan.ts');
  
  const supabase = createClient(
    process.env.SUPABASE_URL!,
    process.env.SUPABASE_SERVICE_KEY!
  );

  const resources = await getPackageResources('neighbourhood-crime-rates');
  const active = resources.filter((r: any) => r.datastore_active);
  
  for await (const batch of paginateCKAN(active[0].id)) {
    const r = batch[0];
    const testRow = {
      neighbourhood: r['AREA_NAME'],
      neighbourhood_id: String(r['HOOD_ID']),
      year: 2024,
      assault_rate: r['ASSAULT_RATE_2024'],
      auto_theft_rate: r['AUTOTHEFT_RATE_2024'],
      break_enter_rate: r['BREAKENTER_RATE_2024'],
      robbery_rate: r['ROBBERY_RATE_2024'],
      shooting_rate: r['SHOOTING_RATE_2024'],
      homicide_rate: r['HOMICIDE_RATE_2024'],
      geom: null,
      ingested_at: new Date().toISOString(),
    };
    console.log('Test row:', JSON.stringify(testRow, null, 2));
    const { error } = await supabase.from('crime_rates').upsert(testRow, { onConflict: 'neighbourhood_id,year' });
    console.log('Error:', error);
    process.exit(0);
  }
}
main();
