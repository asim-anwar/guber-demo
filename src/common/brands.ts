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

const firstWordBrandValidationList = [
    "rich", "rff", "flex", "ultra", "gum", "beauty", "orto", "free", "112", "kin", "happy", "HAPPY"]

const firstOrSecondWordBrandValidationList = ["heel", "contour", "nero", "rsv"]

const ignoreBrandsList = ["bio", "neb"]

export async function getBrandsMapping(): Promise<BrandsMapping> {
    const brandConnections = connections

    // Create a map to track brand relationships
    const brandMap = new Map<string, Set<string>>()

    brandConnections.forEach(({ manufacturer_p1, manufacturers_p2 }) => {
        const brand1 = manufacturer_p1.toLowerCase()
        const brands2 = manufacturers_p2.toLowerCase()
        const brand2Array = brands2.split(";").map((b) => b.trim())
        if (!brandMap.has(brand1)) {
            brandMap.set(brand1, new Set())
        }
        brand2Array.forEach((brand2) => {
            if (!brandMap.has(brand2)) {
                brandMap.set(brand2, new Set())
            }
            brandMap.get(brand1)!.add(brand2)
            brandMap.get(brand2)!.add(brand1)
        })
    })

    // Convert the flat map to an object for easier usage
    const brandsMapping: Record<string, string[]> = {}

    brandMap.forEach((relatedBrands, brand) => {
        brandsMapping[brand] = Array.from(relatedBrands)
    })

    return brandsMapping  
}

function buildCanonicalLookup(brandsMapping: Record<string, string[]>): Record<string, string> {
    const canonicalLookup: Record<string, string> = {}

    for (const [_, relatedBrands] of Object.entries(brandsMapping)) {
        // Filter out ignored brands when selecting canonical
        const filteredBrands = relatedBrands.filter(
            (b) => !ignoreBrandsList.includes(b.toLowerCase())
        )

        // Pick the shortest brand name in the group
        const canonicalBrand = filteredBrands.length > 0
            ? filteredBrands.reduce((shortest, current) =>
                current.length < shortest.length ? current : shortest
            )
            : // fallback to the shortest among all brands if all are ignored
            relatedBrands.reduce((shortest, current) =>
                current.length < shortest.length ? current : shortest
            )

        // Map every brand (in lowercase) to the canonical one
        for (const brand of relatedBrands) {
            canonicalLookup[brand.toLowerCase()] = canonicalBrand.toLowerCase()
        }
    }

    return canonicalLookup
}

async function getPharmacyItems(countryCode: countryCodes, source: sources, versionKey: string, mustExist = true) {
    const finalProducts = items

    return finalProducts
}

export function normalizeInputIfAccentInsensitive(input: string): string {
    const isAccentInsensitive = /[\u0300-\u036f\u00C0-\u017F]/.test(input.normalize("NFD")) // Check for accented characters in the input

    if (isAccentInsensitive) {
        return input.normalize("NFD").replace(/[\u0300-\u036f]/g, "") // Remove accents if any are found
    } else {
        return input // Return the original input if no accents are found
    }
}

export function checkBrandIsSeparateTerm(input: string, brand: string): boolean {
    // Escape any special characters in the brand name for use in a regular expression
    let escapedBrand = brand.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")

    if (escapedBrand === "happy") escapedBrand = escapedBrand.toUpperCase() // Special case for "happy" brand to match uppercase in input

    input = normalizeInputIfAccentInsensitive(input) // Normalize input for accent insensitivity
    
    // Check if the brand is at the beginning or end of the string
    const atBeginningOrEndOrMiddle = new RegExp(
        `^(?:${escapedBrand}\\s|.*\\s${escapedBrand}\\s.*|.*\\s${escapedBrand})$`,
        "i"
    ).test(input)

    if(firstWordBrandValidationList.includes(brand)){ // Additional check for brands that are common words at the start
        const atBeginning = new RegExp(`^${escapedBrand}(\\b|\\s|$)`, "i").test(input); // Check if the brand is at the beginning of the title string

        if(!atBeginning){
            return false // If the brand is not at the beginning, return false
        }
    }

    if(firstOrSecondWordBrandValidationList.includes(brand)){ // Additional check for brands that are common words at the second position
        const atBeginning = new RegExp(`^${escapedBrand}(\\b|\\s|$)`, "i").test(input); // Check if the brand is at the beginning of the title string
        const atSecondPosition = new RegExp(`^\\S+\\s+${escapedBrand}(\\b|\\s|$)`, "i").test(input);
        if(!atBeginning && !atSecondPosition){
            return false // If the brand is not at the beginning or second position, return false
        }
    }
    
    // Check if the brand is a separate term in the string
    const separateTerm = new RegExp(`\\b${escapedBrand}\\b`, "i").test(input)

    // The brand should be at the beginning, end, or a separate term
    return atBeginningOrEndOrMiddle || separateTerm
}

export async function assignBrandIfKnown(countryCode: countryCodes, source: sources, job?: Job) {
    const context = { scope: "assignBrandIfKnown" } as ContextType

    const brandsMapping = await getBrandsMapping()
    const canonicalLookup = buildCanonicalLookup(brandsMapping)

    const versionKey = "assignBrandIfKnown"
    let products = await getPharmacyItems(countryCode, source, versionKey, false)
    let counter = 0
    let results: any[] = []
    for (let product of products) {
        counter++
        let mainBrand: string = null

        // if (product.m_id) {
        //     // Already exists in the mapping table, probably no need to update
        //     continue
        // }

        let matchedBrands = []
        for (const brandKey in brandsMapping) {
            const relatedBrands = brandsMapping[brandKey]
            for (const brand of relatedBrands) {
                if (matchedBrands.includes(brand) || ignoreBrandsList.includes(brand)) {
                    continue
                }
                const isBrandMatch = checkBrandIsSeparateTerm(product.title, brand)
                if (isBrandMatch) {
                    matchedBrands.push(brand)
                }
            }
        }

         let priorityBrand: string = null

        if (matchedBrands.length > 0) {
            const allConnected = new Set<string>()

            // Find all related brands that connect through the mapping
            matchedBrands.forEach((b) => {
            const related = brandsMapping[b.toLowerCase()] || []
            related.forEach((r) => allConnected.add(r.toLowerCase()))
            })

            // Merge matched brands + all connected ones
            const allPossible = new Set([...matchedBrands.map((b) => b.toLowerCase()), ...allConnected])

            // Choose the brand that appears first in the product title as the priority brand, this solves the issue of multiple brands being matched
            priorityBrand = Array.from(allPossible).reduce((best, b) => {
                const idx = product.title.toLowerCase().indexOf(b)
                if (idx === -1) return best
                if (best === null) return b
                return product.title.toLowerCase().indexOf(best) < idx ? best : b
            }, null)

            if(!priorityBrand){
                priorityBrand = matchedBrands[0].toLowerCase()
            }

            mainBrand = canonicalLookup[priorityBrand.toLowerCase()] || priorityBrand.toLowerCase()

            results.push({
                productTitle: product.title,
                matchedBrands,
                priorityBrand,
                assignedBrand: mainBrand
            })
        }

        console.log(`${product.title} -> ${priorityBrand} (matched: ${matchedBrands.join(", ")})`)
        const sourceId = product.source_id
        const meta = { matchedBrands }
        const brand = mainBrand

        const key = `${source}_${countryCode}_${sourceId}`
        const uuid = stringToHash(key)

        // Then brand is inserted into product mapping table
    }

    const filePath = `./results/brand_results_${countryCode}_${source}.json` // Define the file path for saving results
    fs.mkdirSync("./results", { recursive: true }) // Ensure the results directory exists
    fs.writeFileSync(filePath, JSON.stringify(results, null, 2), "utf-8") // Write results to the file
}
