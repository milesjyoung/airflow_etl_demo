import asyncio
from typing import List, Dict
from playwright.async_api import async_playwright, Page, TimeoutError as PlaywrightTimeoutError

BASE_URL = "https://www.cslb.ca.gov/OnlineServices/CheckLicenseII/CheckLicense.aspx"

async def scrape_single_license(page: Page, cslb: str) -> Dict:
    result = {"CSLB": cslb, "Installer Name": None, "error": None}

    try:
        await page.goto(BASE_URL)
        await page.locator("#MainContent_LicNo").fill(str(cslb))
        await page.locator('#MainContent_Contractor_License_Number_Search').click()
        print("pass clicked")
        td_locator = page.locator("td#MainContent_BusInfo")
        if await td_locator.count() > 0:
            full_text = await td_locator.inner_html()
            first_line = full_text.split("<br>")[0].strip()
            result["Installer Name"] = first_line

    except PlaywrightTimeoutError:
        print(PlaywrightTimeoutError)
        result["error"] = "Timeout waiting for installer name"
    except Exception as e:
        print(e)
        result["error"] = str(e)

    return result

async def _scrape_all(cslbs: List[str]) -> List[Dict]:
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()
        page = await context.new_page()

        results = []
        for cslb in cslbs:
            results.append(await scrape_single_license(page, cslb))

        await browser.close()
        return results

def scrape_licenses(cslbs: List[str]) -> List[Dict]:
    return asyncio.run(_scrape_all(cslbs))