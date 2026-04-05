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
      /traffic\s*signal|streetlight|street\s*light|lamp\s*out|sign\s*missing|sign\s*damage|sign\s*fallen/i,
    category: "roads",
  },
  {
    pattern: /snow\s*removal|ice\s*control|salting|winter\s*maintenance/i,
    category: "roads",
  },

  // ── Noise (before bylaw — noise is a subset of MLS but warrants its own bucket)
  {
    pattern:
      /noise\s*complaint|noise\s*by-?law|loud\s*music|loud\s*party|barking\s*dog|construction\s*noise|noisy/i,
    category: "noise",
  },

  // ── Trees & forestry
  {
    pattern:
      /tree\s*removal|tree\s*inspection|fallen\s*tree|dead\s*tree|damaged\s*tree|tree\s*pruning|tree\s*trim|hazardous\s*tree|tree\s*branch|stump|forestry/i,
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
      /park\s*maintenance|playground|park\s*equipment|trail\s*maintenance|splash\s*pad|park\s*bench|park\s*facility|arena|ice\s*surface|wading\s*pool/i,
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
];

/**
 * Normalize a raw 311 request type string to a ServiceCategory.
 * Returns "other" if no rule matches.
 */
export function normalizeCategory(rawType: string): ServiceCategory {
  if (!rawType) return "other";
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
