import fs from "fs"
import { Job } from "bullmq"
import { countryCodes, dbServers, EngineType } from "../config/enums"
import { ContextType } from "../libs/logger"
import { jsonOrStringForDb, jsonOrStringToJson, stringOrNullForDb, stringToHash } from "../utils"
import _, { lte } from "lodash"
import { sources } from "../sites/sources"
import items from "./../../pharmacyItems.json"
import connections from "./../../brandConnections.json"

type BrandsMapping = {
    [key: string]: string[]
}

type BrandMatchCache = {
    brand: string
    pattern: RegExp
    positionCheck?: 'first' | 'first-or-second'
}

const firstWordBrandValidationList = new Set([
    "rich", "rff", "flex", "ultra", "gum", "beauty", "orto", "free", "112", "kin", "happy", "HAPPY"])

const firstOrSecondWordBrandValidationList = new Set(["heel", "contour", "nero", "rsv"])

const ignoreBrandsList = new Set(["bio", "neb"])

// Optimization:
// Cache for normalized strings to avoid repeated normalization
const normalizationCache = new Map<string, string>()

export async function getBrandsMapping(): Promise<BrandsMapping> {
    const brandConnections = connections

    // Create a map to track brand relationships
    const brandMap = new Map<string, Set<string>>()

    brandConnections.forEach(({ manufacturer_p1, manufacturers_p2 }) => {
        const brand1 = manufacturer_p1.toLowerCase()
        const brands2 = manufacturers_p2.toLowerCase()
        const brand2Array = brands2.split(";").map((b) => b.trim())
        // Initialize sets if not already present
        if (!brandMap.has(brand1)) {
            brandMap.set(brand1, new Set())
        }
        brand2Array.forEach((brand2) => {
            if (!brandMap.has(brand2)) {
                brandMap.set(brand2, new Set())
            }

            // Create bidirectional relationships
            brandMap.get(brand1)!.add(brand2)
            brandMap.get(brand2)!.add(brand1)
        })
    })

    // Converting the flat map to an object for easier usage
    const brandsMapping: Record<string, string[]> = {}

    brandMap.forEach((relatedBrands, brand) => {
        brandsMapping[brand] = Array.from(relatedBrands)
    })

    return brandsMapping  
}

function buildCanonicalLookup(brandsMapping: Record<string, string[]>): Record<string, string> {
    const canonicalLookup: Record<string, string> = {}

    for (const [_, relatedBrands] of Object.entries(brandsMapping)) {
        // Filter out ignored brands when selecting main brand
        const filteredBrands = relatedBrands.filter(
            (b) => !ignoreBrandsList.has(b.toLowerCase())
        )

        // Pick the shortest brand name in the group (this can be adjusted to other criteria, for now I chose shortest)
        const canonicalBrand = filteredBrands.length > 0
            ? filteredBrands.reduce((shortest, current) =>
                current.length < shortest.length ? current : shortest
            )
            : // fallback to the shortest among all brands if all are ignored
            relatedBrands.reduce((shortest, current) =>
                current.length < shortest.length ? current : shortest
            )

        // Map every brand (in lowercase) to the main brand one
        for (const brand of relatedBrands) {
            canonicalLookup[brand.toLowerCase()] = canonicalBrand.toLowerCase()
        }
    }

    return canonicalLookup
}

// Optimization:
// Pre-compile regex patterns for all brands
// This function builds a cache of regex patterns for efficient brand matching
function buildBrandMatchCache(brands: Set<string>): BrandMatchCache[] {
    const cache: BrandMatchCache[] = []

    for (const brand of brands) {
        if (ignoreBrandsList.has(brand)) continue

        let escapedBrand = brand.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")
        
        // Special case: "happy" brand must match uppercase in input
        if (escapedBrand === "happy") {
            escapedBrand = escapedBrand.toUpperCase()
        }

        let positionCheck: 'first' | 'first-or-second' | undefined

        if (firstWordBrandValidationList.has(brand)) {
            // Must be first word
            positionCheck = 'first'
            cache.push({
                brand,
                pattern: new RegExp(`^${escapedBrand}(\\b|\\s|$)`, "i"),
                positionCheck
            })
        } else if (firstOrSecondWordBrandValidationList.has(brand)) {
            // Must be first or second word - use alternation for single regex
            positionCheck = 'first-or-second'
            cache.push({
                brand,
                pattern: new RegExp(`^${escapedBrand}(\\b|\\s|$)|^\\S+\\s+${escapedBrand}(\\b|\\s|$)`, "i"),
                positionCheck
            })
        } else {
            // Must be a separate term anywhere in the string
            cache.push({
                brand,
                pattern: new RegExp(`\\b${escapedBrand}\\b`, "i")
            })
        }
    }

    return cache
}

async function getPharmacyItems(countryCode: countryCodes, source: sources, versionKey: string, mustExist = true) {
    const finalProducts = items

    return finalProducts
}

// Optimization:
// Using caching to avoid repeated normalization of the same string
export function normalizeInputIfAccentInsensitive(input: string): string {
    const cached = normalizationCache.get(input)
    if (cached !== undefined) return cached

    // Check if string contains accented characters
    const normalized = input.normalize("NFD")
    const hasAccents = /[\u0300-\u036f\u00C0-\u017F]/.test(normalized)

    let result: string
    if (hasAccents) {
        // Remove combining diacritical marks
        result = normalized.replace(/[\u0300-\u036f]/g, "")
    } else {
        result = input
    }

    // Cache the result
    normalizationCache.set(input, result)
    return result
}

function checkBrandMatch(normalizedInput: string, cacheEntry: BrandMatchCache): boolean {
    return cacheEntry.pattern.test(normalizedInput)
}

export async function assignBrandIfKnown(
    countryCode: countryCodes,
    source: sources,
    job?: Job
) {
    const context = { scope: "assignBrandIfKnown" } as ContextType
    
    // Load brand relationships and canonical mappings
    const brandsMapping = await getBrandsMapping()
    const canonicalLookup = buildCanonicalLookup(brandsMapping)

    // Get all brands as a flat set
    const allBrands = new Set<string>()
    Object.values(brandsMapping).forEach(brands => {
        brands.forEach(brand => allBrands.add(brand))
    })

    // Optimization:
    // Pre-compile all regex patterns for brands
    const brandMatchCache = buildBrandMatchCache(allBrands)
    
    // Optimization:
    // Sort by brand length (descending) to match longer brands first
    // This prevents "bio" matching before "biofarm" for example
    brandMatchCache.sort((a, b) => b.brand.length - a.brand.length)

    // Optimization:
    // Pre-compute the full brand network for each brand
    const brandNetworkCache = new Map<string, Set<string>>()
    for (const brand of allBrands) {
        const network = new Set<string>()
        const related = brandsMapping[brand.toLowerCase()] || []
        related.forEach(r => network.add(r.toLowerCase()))
        network.add(brand.toLowerCase())
        brandNetworkCache.set(brand.toLowerCase(), network)
    }

    const versionKey = "assignBrandIfKnown"
    const products = await getPharmacyItems(countryCode, source, versionKey, false)
    const results: any[] = []

    // Process each product
    for (let i = 0; i < products.length; i++) {
        const product = products[i]
        
        // Normalize once per product
        const normalizedTitle = normalizeInputIfAccentInsensitive(product.title)
        const lowerTitle = normalizedTitle.toLowerCase()
        
        const matchedBrands: string[] = []
        const matchedBrandSet = new Set<string>() // For lookup of already matched brands

        // Optimization:
        // Check each brand against the product title using pre-compiled patterns
        for (const cacheEntry of brandMatchCache) {
            // Skip if brand already matched
            if (matchedBrandSet.has(cacheEntry.brand)) {
                continue
            }

            if (checkBrandMatch(normalizedTitle, cacheEntry)) {
                matchedBrands.push(cacheEntry.brand)
                matchedBrandSet.add(cacheEntry.brand)
            }
        }

        let mainBrand: string | null = null
        let priorityBrand: string | null = null

        if (matchedBrands.length > 0) {
            // Collect all related brands using pre-computed network
            const allConnectedBrands = new Set<string>()
            matchedBrands.forEach(brand => {
                const network = brandNetworkCache.get(brand.toLowerCase())
                if (network) {
                    network.forEach(b => allConnectedBrands.add(b))
                }
            })

            // Select the brand that appears earliest in the title
            let earliestIndex = Infinity

            for (const brand of allConnectedBrands) {
                const index = lowerTitle.indexOf(brand)
                if (index !== -1 && index < earliestIndex) {
                    earliestIndex = index
                    priorityBrand = brand
                }
            }

            // Fallback to first matched brand if no priority found
            if (!priorityBrand) {
                priorityBrand = matchedBrands[0].toLowerCase()
            }

            // Map to a main brand name
            mainBrand = canonicalLookup[priorityBrand] || priorityBrand
        }

        // Prepare data for storage
        const sourceId = product.source_id
        const meta = { matchedBrands }
        const brand = mainBrand
        
        const key = `${source}_${countryCode}_${sourceId}`
        const uuid = stringToHash(key)

        // Store result
        results.push({
            product: product.title,
            matchedBrands,
            assignedBrand: mainBrand,
            priorityBrand: priorityBrand,
        })

        // TODO: Insert brand into product mapping table
    }

    // Export results to JSON files
    const resultsPath = `./brand_results_${countryCode}_${source}.json`

    fs.writeFileSync(resultsPath, JSON.stringify(results, null, 2), "utf-8")


    const brandsAssigned = results.filter(r => r.brand).length
    console.log(`✓ Processed ${products.length} products`)
    console.log(`✓ Assigned brands to ${brandsAssigned} products (${((brandsAssigned / products.length) * 100).toFixed(1)}%)`)
    
    // Clear cache after processing
    normalizationCache.clear()
}
