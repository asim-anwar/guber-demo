import { Job } from "bullmq"
import { countryCodes, dbServers, EngineType } from "../config/enums"
import { ContextType } from "../libs/logger"
import { jsonOrStringForDb, jsonOrStringToJson, stringOrNullForDb, stringToHash } from "../utils"
import _ from "lodash"
import { sources } from "../sites/sources"
import items from "./../../pharmacyItems.json"
import connections from "./../../brandConnections.json"

type BrandsMapping = {
    [key: string]: string[]
}

const firstWordValidationList = [
    "rich", "rff", "flex", "ultra", "gum", "beauty", "orto", "free", "112", "kin", "happy", "heel", "contour", "nero", "rsv"
]

const secondWordValidationList = ["heel", "contour", "nero", "rsv"]

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
    const flatMapObject: Record<string, string[]> = {}

    brandMap.forEach((relatedBrands, brand) => {
        flatMapObject[brand] = Array.from(relatedBrands)
    })

    return flatMapObject
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
    const escapedBrand = brand.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")

    input = normalizeInputIfAccentInsensitive(input) // Normalize input for accent insensitivity
    
    // Check if the brand is at the beginning or end of the string
    const atBeginningOrEndOrMiddle = new RegExp(
        `^(?:${escapedBrand}\\s|.*\\s${escapedBrand}\\s.*|.*\\s${escapedBrand})$`,
        "i"
    ).test(input)

    if(firstWordValidationList.includes(brand)){ // Additional check for brands that are common words at the start
        const atBeginning = new RegExp(`^${escapedBrand}(\\b|\\s|$)`, "i").test(input); // Check if the brand is at the beginning of the title string

        if(!atBeginning){
            return false // If the brand is not at the beginning, return false
        }
    }


    const at2ndPosition = new RegExp(`^\\S+\\s+${escapedBrand}(\\b|\\s|$)`, "i").test(input);
    
    // Check if the brand is a separate term in the string
    const separateTerm = new RegExp(`\\b${escapedBrand}\\b`, "i").test(input)
    if ((input.includes('LIVOL EXTRA') || input.includes('GUM Travel') || input.includes('BABE') || input.includes('HAPPY')) && (atBeginningOrEndOrMiddle === true || separateTerm === true)) {
        console.log('Checking brand:', brand, 'in input:', input);
        console.log('Escaped brand:', escapedBrand);
        console.log({
        input,
        brand,
        atBeginningOrEndOrMiddle,
        at2ndPosition,
        separateTerm
    });
    }

    // The brand should be at the beginning, end, or a separate term
    return atBeginningOrEndOrMiddle || separateTerm
}

export async function assignBrandIfKnown(countryCode: countryCodes, source: sources, job?: Job) {
    const context = { scope: "assignBrandIfKnown" } as ContextType

    const brandsMapping = await getBrandsMapping()

    const versionKey = "assignBrandIfKnown"
    let products = await getPharmacyItems(countryCode, source, versionKey, false)
    let counter = 0
    for (let product of products) {
        counter++

        // if (product.m_id) {
        //     // Already exists in the mapping table, probably no need to update
        //     continue
        // }

        let matchedBrands = []
        for (const brandKey in brandsMapping) {
            const relatedBrands = brandsMapping[brandKey]
            for (const brand of relatedBrands) {
                if (matchedBrands.includes(brand)) {
                    continue
                }
                const isBrandMatch = checkBrandIsSeparateTerm(product.title, brand)
                if (isBrandMatch) {
                    matchedBrands.push(brand)
                }
            }
        }
        console.log(`${product.title} -> ${_.uniq(matchedBrands)}`)
        const sourceId = product.source_id
        const meta = { matchedBrands }
        const brand = matchedBrands.length ? matchedBrands[0] : null

        const key = `${source}_${countryCode}_${sourceId}`
        const uuid = stringToHash(key)

        // Then brand is inserted into product mapping table
    }
}
