/**
 * normalizer.ts
 *
 * The core IP of the product. Maps 850+ raw 311 request type strings
 * from Toronto's CKAN dataset into 8 clean, user-facing categories.
 *
 * The mapping is regex-based and ordered by specificity — more specific
 * patterns first, with "other" as the fallback. Update this file as
 * new request types appear in the dataset.
 *
 * This normalization work is what an acquirer cannot easily replicate —
 * it requires manual review of hundreds of edge cases.
 */

export type ServiceCategory =
  | "roads"    // Potholes, road damage, sidewalks, signals, signs
  | "water"    // Flooding, water mains, sewers, catch basins
  | "noise"    // Noise complaints (residential, construction, commercial)
  | "bylaw"    // Property standards, graffiti, licensing, pests, animals
  | "trees"    // Fallen/hazardous trees, pruning, removal
  | "waste"    // Missed collections, illegal dumping, bulk items
  | "parks"    // Park & playground maintenance, recreation facilities
  | "other";   // Uncategorized — review periodically and remap

interface CategoryRule {
  pattern: RegExp;
  category: ServiceCategory;
}

/**
 * Rules are evaluated top-to-bottom. First match wins.
 * Keep more specific patterns above broader ones.
 */
const RULES: CategoryRule[] = [
  // ── Water & flooding (before roads — catch basins are water, not roads)
  {
    pattern:
      /water\s*main|watermain|catch\s*basin|sewer|flood|storm\s*drain|water\s*service|hydrant|water\s*line|water\s*break|water\s*leak/i,
    category: "water",
  },
  {
    pattern: /basement\s*flood|surface\s*flood|overland\s*flood/i,
    category: "water",
  },

  // ── Roads & transportation
  { pattern: /pot\s*hole|road\s*-\s*pot/i, category: "roads" },
  {
    pattern:
      /pothole|road\s*damage|road\s*surface|asphalt|pavement|cracking|alligator/i,
    category: "roads",
  },
  {
    pattern:
      /sidewalk|curb|boulevard\s*repair|road\s*resurfac|lane\s*marking|road\s*work/i,
    category: "roads",
  },
  {
    pattern:
      /traffic\s*signal|traffic\s+control\s+signal|signal\s*timing|traffic\s*calm|streetlight|street\s*light|lamp\s*out|sign\s*missing|sign\s*damage|sign\s*fallen/i,
    category: "roads",
  },
  {
    pattern: /snow\s*removal|ice\s*control|salting|winter\s*maintenance/i,
    category: "roads",
  },
  {
    pattern:
      /road\s*plow|road\s*plough|road\s*-\s*clean|road\s*-\s*sink|road\s*-\s*damage|clean\s*up\s*debris|maintenance\s*holes?|sink\s*hole|roadway\s*utility\s*cut|bus\s*stops?\s*snow|snow\s*at\s*intersection|school\s*zone\s*snow|storm\s*clean\s*up|intersection\s*safety|expressway|pxo\s*maintenance|pedestrian\s*crossover|walkway.*damage|laneway.*damage/i,
    category: "roads",
  },
  {
    pattern: /outcome\s*of\s*service.*road|complaint.*outcome|complaint.*time\s*line/i,
    category: "roads",
  },
  {
    pattern:
      /missing.*sign|damaged.*sign|street.*sign|traffic\s*sign|investigate.*sign|temporary.*sign|^signs$/i,
    category: "roads",
  },

  // ── Noise (before bylaw — noise is a subset of MLS but warrants its own bucket)
  {
    pattern:
      /noise\s*complaint|noise\s*by-?law|loud\s*music|loud\s*party|barking\s*dog|construction\s*noise|noisy|motor\s*vehicle\s*noise|moving.*noise|amplified\s*sound|unreasonable.*noise/i,
    category: "noise",
  },

  // ── Trees & forestry
  {
    pattern:
      /tree\s*removal|tree\s*inspection|fallen\s*tree|dead\s*tree|damaged\s*tree|tree\s*pruning|tree\s*trim|hazardous\s*tree|tree\s*branch|stump|forestry|general\s*pruning|tree\s*emergency|boulevards.*grass|grass\s*cutting/i,
    category: "trees",
  },

  // ── Waste & solid waste management
  {
    pattern:
      /missed\s*collection|missed\s*pick.?up|garbage\s*collection|recycling\s*collection|green\s*bin|blue\s*bin|compost/i,
    category: "waste",
  },
  {
    pattern:
      /illegal\s*dump|dumping|bulk\s*item|litter|waste\s*container|bin\s*repair|bin\s*missing|overflowing/i,
    category: "waste",
  },

  // ── Parks & recreation
  {
    pattern:
      /park\s*maintenance|park\s*use|playground|park\s*equipment|trail\s*maintenance|splash\s*pad|park\s*bench|park\s*facility|arena|ice\s*surface|wading\s*pool|boulevards.*weed|weed\s*removal/i,
    category: "parks",
  },

  // ── Bylaw, property standards, licensing (broadest MLS bucket — keep last)
  {
    pattern:
      /property\s*standard|exterior\s*standard|interior\s*standard|zoning|property\s*clean|property\s*condition/i,
    category: "bylaw",
  },
  {
    pattern:
      /graffiti|vandalism|tagging/i,
    category: "bylaw",
  },
  {
    pattern:
      /animal\s*service|stray|raccoon|rodent|pest|rat\s*infestation|bed\s*bug|wasp|wildlife/i,
    category: "bylaw",
  },
  { pattern: /cadaver|dead\s*animal|carcass/i, category: "bylaw" },
  {
    pattern:
      /licensing|rooming\s*house|body\s*rub|refreshment\s*vehicle|food\s*truck|mobile\s*sign/i,
    category: "bylaw",
  },
  {
    pattern:
      /staff\s*service.*comp(laint|liment)|public\s*spaces|complaint.*contractor|contractor\s*complaint|encroachment|crossing\s*guard|dogs?\s*off\s*leash|illegal.*parking|postering|election\s*signs?|comment.*suggestion/i,
    category: "bylaw",
  },
];

/**
 * Exact-string overrides (case-insensitive key lookup).
 * Checked before regex rules — use for high-volume strings that resist
 * pattern matching, or to explicitly pin noisy/unknown types to "other".
 */
const EXACT_MATCHES: Record<string, ServiceCategory> = {
  "road plowing request":                              "roads",
  "road - cleaning/debris":                            "roads",
  "clean up debris on road":                           "roads",
  "missing/damaged signs":                             "roads",
  "missing / damaged street or traffic signs":         "roads",
  "road - sinking":                                    "roads",
  "maintenance hole-damage":                           "roads",
  "expressway requires cleaning.":                     "roads",
  "expressway requires cleaning":                      "roads",
  "pxo maintenance":                                   "roads",
  "street furniture request":                          "roads",
  "amplified sound":                                   "noise",
  "moving motor vehicle noise":                        "noise",
  "park use":                                          "parks",
  "election signs":                                    "bylaw",
  "complaint - crossing guard conduct":                "bylaw",
  "staff service - complaint - 311 toronto":           "bylaw",
  "staff service compliment":                          "bylaw",
  // Parks
  "dog off-leash in a city park":                      "parks",
  "parks ravine safety mtc fnem":                      "parks",
  "park garbage bin overflowing":                      "parks",
  "park lighting maintenance":                         "parks",
  "park conduct":                                      "parks",
  "park litter and garbage":                           "parks",
  "illegal dumping in park":                           "parks",
  "park property snow and ice clearing":               "parks",
  "park pathways and trails maintenance":              "parks",

  // Waste (park-specific overflow bins)
  "garbage / park / bin overflow":                     "waste",
  "recycle / park / bin overflow":                     "waste",

  // Bylaw — parking
  "illegal off-street parking":                        "bylaw",
  "illegal on-street parking":                         "bylaw",
  "general parking regulations":                       "bylaw",
  "blocked access by parking":                         "bylaw",
  "time limit or excessive duration parking":          "bylaw",
  "corner parking prohibition":                        "bylaw",
  "parking in a public lane":                          "bylaw",

  // Trees
  "residential or park tree removal":                  "trees",

  // Explicit no-ops
  "unknown - mlsblemmvn":                              "other",
  "unknown - ts-stfurn-req":                           "other",
  "unknown - tso-cmp04":                               "other",
};

/**
 * Normalize a raw 311 request type string to a ServiceCategory.
 * Returns "other" if no rule matches.
 */
export function normalizeCategory(rawType: string): ServiceCategory {
  if (!rawType) return "other";
  const exact = EXACT_MATCHES[rawType.toLowerCase().trim()];
  if (exact) return exact;
  for (const { pattern, category } of RULES) {
    if (pattern.test(rawType)) return category;
  }
  return "other";
}

/**
 * Given an array of unique raw type strings, return a lookup map.
 * Run this once on startup or during ingestion to avoid re-running
 * regex on every record.
 *
 * @example
 * const lookup = buildCategoryLookup(allUniqueTypes)
 * const category = lookup[record.rawType] ?? 'other'
 */
export function buildCategoryLookup(
  rawTypes: string[]
): Record<string, ServiceCategory> {
  return Object.fromEntries(
    rawTypes.map((t) => [t, normalizeCategory(t)])
  );
}

/**
 * Human-readable labels for each category.
 * Used in the report UI and API response.
 */
export const CATEGORY_LABELS: Record<ServiceCategory, string> = {
  roads:  "Roads & sidewalks",
  water:  "Water & flooding",
  noise:  "Noise complaints",
  bylaw:  "Bylaw & property",
  trees:  "Trees & forestry",
  waste:  "Waste & dumping",
  parks:  "Parks & recreation",
  other:  "Other",
};
