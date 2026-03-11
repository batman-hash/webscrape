import time
import random
import re
import logging
import base64
from datetime import datetime, timezone
from urllib.parse import urlparse, parse_qs, quote_plus
import os
from playwright.sync_api import sync_playwright, TimeoutError
from openpyxl import Workbook

# --------------------------------------------------
# 1. LOGGING & CONFIG
# --------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

SEARCH_QUERIES = [
    "aziende chimiche" "Emilia Romagna",
    "industria chimica" "Emilia Romagna",
    "prodotti chimici" "Emilia Romagna",
    "elenco aziende chimiche Emilia Romagna",
    "associazione industrie chimiche Emilia Romagna",
    "distretto chimico Emilia Romagna",
    "registro imprese chimiche Emilia Romagna",
    "cluster chimico Emilia Romagna"
]
TARGET_WEBSITES = 30
MAX_IP_ROTATIONS = 10
RESULTS_PER_PAGE = 10
MAX_SEARCH_PAGES_PER_ID = 5
MAX_VISITS = 40
HEADLESS = True
NAV_TIMEOUT_MS = 25000
PROXIES = [] # Fill with {"server": "http://proxy:port"} if needed

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"

# --------------------------------------------------
# 2. REGEXES & HEURISTICS
# --------------------------------------------------
EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")
PHONE_RE = re.compile(r"(?:(?:\+39|0039)\s?)?(?:0\d{1,4}[\s./-]?\d{5,8}|\d{3}[\s./-]?\d{6,8})")
CAP_RE = re.compile(r"\b\d{5}\b")
ER_CITIES_RE = re.compile(
    r"\b(Bologna|Modena|Parma|Reggio Emilia|Ravenna|Ferrara|Rimini|Forlì|Cesena|Piacenza|Imola|Carpi|Faenza|Sassuolo|Casalecchio)\b", 
    re.IGNORECASE
)
ADDRESS_RE = re.compile(
    r"\b(?:Via|Viale|Piazza|Corso|Largo|Strada|S\.?S\.?|SS|SP|Località|Loc\.?)\s+[A-Za-zÀ-ÖØ-öø-ÿ0-9'’().,\-\/ ]{6,}",
    re.IGNORECASE
)

def get_pmi_heuristic(text: str) -> str:
    t = text.lower()
    if any(k in t for k in ["multinazionale", "global", "group", "gruppo", "leader mondiale"]):
        return "Grande Impresa"
    if any(k in t for k in ["artigiana", "piccola", "familiare", "ditta individuale"]):
        return "Micro / Piccola"
    return "Media Impresa (Stima)"

# --------------------------------------------------
# 3. HELPERS & RESOURCE OPTIMIZATION
# --------------------------------------------------
def human_delay(a=1.1, b=2.8):
    time.sleep(random.uniform(a, b))

def launch_browser(p, proxy=None):
    browser = p.chromium.launch(headless=HEADLESS)
    context = browser.new_context(user_agent=USER_AGENT, proxy=proxy)
    # Speed up resources: block images/media/fonts
    context.route("**/*", lambda route, req: route.abort() if req.resource_type in ("image", "media", "font") else route.continue_())
    return browser, context

def extract_real_bing_url(href: str) -> str:
    try:
        parsed = urlparse(href)
        if "bing.com" in parsed.netloc and "/ck/" in parsed.path:
            qs = parse_qs(parsed.query)
            if "u" in qs:
                encoded = qs["u"][0].split("a1")[-1] # Simple strip if needed
                padded = encoded + "=" * ((4 - len(encoded) % 4) % 4)
                return base64.b64decode(padded).decode("utf-8", errors="ignore")
        return href
    except: return href

# --------------------------------------------------
# 4. SEARCH PHASE (Bing)
# --------------------------------------------------
def collect_websites_from_bing(p) -> list[str]:
    collected = {}
    attempt = 0
    while len(collected) < TARGET_WEBSITES and attempt < MAX_IP_ROTATIONS:
        proxy = random.choice(PROXIES) if PROXIES else None
        browser, context = launch_browser(p, proxy)
        page = context.new_page()
        try:
            for q in SEARCH_QUERIES:
                if len(collected) >= TARGET_WEBSITES: break
                for page_num in range(MAX_SEARCH_PAGES_PER_ID):
                    offset = page_num * RESULTS_PER_PAGE
                    page.goto(f"https://www.bing.com/search?q={quote_plus(q)}&first={offset}", timeout=NAV_TIMEOUT_MS)
                    page.wait_for_timeout(1000)
                    
                    if "captcha" in page.content().lower(): raise Exception("Blocked")
                    
                    anchors = page.query_selector_all("li.b_algo h2 a")
                    for a in anchors:
                        href = a.get_attribute("href")
                        real = extract_real_bing_url(href)
                        if real and "http" in real and "bing.com" not in real:
                            domain = urlparse(real).netloc
                            if domain not in collected: collected[domain] = real
                    human_delay()
        except Exception as e: logger.warning(f"Rotation triggered: {e}")
        finally: browser.close()
        attempt += 1
    return list(collected.values())

# --------------------------------------------------
# 5. SCRAPE PHASE
# --------------------------------------------------
def deep_scrape(p, urls: list[str]) -> list[dict]:
    results = []
    browser, context = launch_browser(p)
    page = context.new_page()
    for idx, url in enumerate(urls[:MAX_VISITS], 1):
        logger.info(f"[{idx}] Scraping {url}")
        try:
            page.goto(url, timeout=NAV_TIMEOUT_MS, wait_until="domcontentloaded")
            page.wait_for_timeout(1000)
            text = page.inner_text("body")
            
            email = EMAIL_RE.search(text)
            phone = PHONE_RE.search(text)
            cap = CAP_RE.search(text)
            city = ER_CITIES_RE.search(text)
            addr = ADDRESS_RE.search(text)
            
            try: name = re.split(r'[-|:]', page.title())[0].strip()
            except: name = "N/A"

            results.append({
                "denominazione": name,
                "pmi": get_pmi_heuristic(text),
                "phone": phone.group() if phone else "N/A",
                "email": email.group() if email else "N/A",
                "address": addr.group().split("  ")[0] if addr else "N/A",
                "city": city.group().capitalize() if city else "N/A",
                "cap": cap.group() if cap else "N/A",
                "website": url,
                "notes": "Auto-extracted"
            })
        except: logger.warning(f"Skip {url}")
        human_delay(1, 2)
    browser.close()
    return results

# --------------------------------------------------
# 6. EXPORT
# --------------------------------------------------


def export_xlsx(rows, filename):
    wb = Workbook()
    ws = wb.active
    ws.title = "Aziende Lead"

    headers = [
        "Denominazione", "PMI Category", "Telefono", "Email",
        "Indirizzo", "Città", "CAP", "Sito Web", "Note"
    ]
    ws.append(headers)

    for r in rows:
        ws.append([
            r.get("denominazione", ""),
            r.get("pmi", ""),
            r.get("phone", ""),
            r.get("email", ""),
            r.get("address", ""),
            r.get("city", ""),
            r.get("cap", ""),
            r.get("website", ""),
            r.get("notes", "")
        ])

    # 🔍 percorso assoluto per debug
    abs_path = os.path.abspath(filename)
    print("Saving to:", abs_path)

    # 🛡️ se il file esiste, crea nome unico
    if os.path.exists(abs_path):
        base, ext = os.path.splitext(abs_path)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        abs_path = f"{base}_{ts}{ext}"
        print("File exists, new name:", abs_path)

    # 💾 salvataggio sicuro
    wb.save(abs_path)


# --------------------------------------------------
# 7. MAIN
# --------------------------------------------------
def run():
    with sync_playwright() as p:
        logger.info("🚀 Starting...")
        sites = collect_websites_from_bing(p)
        logger.info(f"Found {len(sites)} sites. Starting deep scrape...")
        data = deep_scrape(p, sites)
        fname = f"leads_chimica_ER_{datetime.now().strftime('%Y%m%d')}.xlsx"
        export_xlsx(data, fname)
        logger.info(f"✅ Done! File: {fname}")

if __name__ == "__main__":
    run()