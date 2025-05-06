import streamlit as st
# from bs4 import BeautifulSoup # Still useful for sitemap parsing
from urllib.parse import urlparse
import pandas as pd
import re
import time
import asyncio
import aiohttp

# Selenium imports
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager # Eases driver management

# --- Constants and Configuration ---
USER_AGENT_HTTP = "WhatsAppLinkExtractor_SitemapFetcher/2.2"
USER_AGENT_SELENIUM = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36" # More browser-like for Selenium
WHATSAPP_LINK_PATTERN = re.compile(r"https?://chat\.whatsapp\.com/[A-Za-z0-9\-_]+")

# --- Helper Functions (some from previous versions) ---
def is_valid_url(url):
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except ValueError:
        return False

def get_domain(url):
    try:
        return urlparse(url).netloc
    except Exception:
        return None

async def fetch_content_async(session, url, delay, headers, log_area):
    await asyncio.sleep(delay)
    try:
        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=20)) as response:
            response.raise_for_status()
            return await response.text()
    except Exception as e:
        if log_area: log_area.warning(f"HTTP Error fetching sitemap/page list for {url}: {e}")
        return None

async def get_urls_from_sitemap_xml_bs(sitemap_xml_content, log_area): # Using BS4 for XML
    from bs4 import BeautifulSoup # Import here if only used here
    page_urls = set()
    sub_sitemap_urls = set()
    try:
        soup = BeautifulSoup(sitemap_xml_content, 'xml')
        for loc_tag in soup.find_all('loc'):
            url = loc_tag.string.strip()
            if url.endswith('.xml'):
                sub_sitemap_urls.add(url)
            elif is_valid_url(url):
                page_urls.add(url)
        if log_area: log_area.info(f"Parsed sitemap: {len(page_urls)} pages, {len(sub_sitemap_urls)} sub-sitemaps.")
    except Exception as e:
        if log_area: log_area.error(f"Error parsing sitemap XML: {e}")
    return page_urls, sub_sitemap_urls


async def fetch_all_page_urls_from_sitemaps_recursive_bs(initial_sitemap_url, session, delay, headers, log_area):
    from collections import deque # Import here if only used here
    all_page_urls_found = set()
    sitemap_queue = deque([initial_sitemap_url])
    processed_sitemaps = set()

    while sitemap_queue:
        current_sitemap_url = sitemap_queue.popleft()
        if current_sitemap_url in processed_sitemaps:
            continue
        processed_sitemaps.add(current_sitemap_url)
        if log_area: log_area.info(f"Fetching sitemap: {current_sitemap_url}")
        sitemap_content = await fetch_content_async(session, current_sitemap_url, delay, headers, log_area)
        if sitemap_content:
            page_urls, sub_sitemap_urls = await get_urls_from_sitemap_xml_bs(sitemap_content, log_area)
            all_page_urls_found.update(page_urls)
            for sub_sitemap in sub_sitemap_urls:
                if sub_sitemap not in processed_sitemaps:
                    sitemap_queue.append(sub_sitemap)
    return all_page_urls_found

# --- Selenium Core Logic ---
def setup_selenium_driver():
    """Sets up and returns a Selenium WebDriver."""
    chrome_options = Options()
    chrome_options.add_argument("--headless") # Run in background
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu") # Often recommended for headless
    chrome_options.add_argument(f"user-agent={USER_AGENT_SELENIUM}")
    
    # This is the tricky part for Streamlit Cloud.
    # webdriver_manager handles download locally.
    # For Streamlit Cloud, you'd need chromedriver in PATH or specify its location.
    try:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        return driver
    except Exception as e:
        st.error(f"Failed to initialize Selenium WebDriver: {e}. "
                 "Ensure ChromeDriver is installed and accessible, or you are running in an environment that supports it (like local).")
        return None

def scrape_page_with_selenium(driver, page_url, wait_time_after_load, join_button_text_selector, status_log):
    """
    Navigates to a page, optionally clicks a join button, waits, and gets the final URL.
    """
    try:
        status_log.info(f"Selenium: Navigating to {page_url}")
        driver.get(page_url)
        
        # Wait for initial page elements if necessary (e.g., body to be present)
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "body")))

        if join_button_text_selector:
            status_log.info(f"Selenium: Looking for join button with text/selector: '{join_button_text_selector}'")
            try:
                # Try finding by partial link text first (common for "Join Group")
                # You might need more robust selectors (CSS selector, XPath)
                join_button = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.PARTIAL_LINK_TEXT, join_button_text_selector))
                )
                # Alternative selectors (more complex to implement generically here):
                # join_button = driver.find_element(By.XPATH, f"//*[contains(text(), '{join_button_text_selector}') and (self::a or self::button)]")
                # join_button = driver.find_element(By.CSS_SELECTOR, join_button_text_selector) # If it's a CSS selector

                status_log.info(f"Selenium: Found join button, clicking...")
                join_button.click()
                # Wait for potential navigation or new content after click
                time.sleep(1) # Small fixed wait after click, adjust as needed
            except Exception as e:
                status_log.warning(f"Selenium: Could not find or click join button '{join_button_text_selector}' on {page_url}. Error: {e}. Proceeding to wait on current page.")
        
        status_log.info(f"Selenium: Waiting {wait_time_after_load}s for JS redirect on {driver.current_url} (was {page_url})")
        time.sleep(wait_time_after_load) # Wait for the JS timer

        final_url = driver.current_url
        status_log.info(f"Selenium: Final URL after wait: {final_url}")

        match = WHATSAPP_LINK_PATTERN.search(final_url)
        if match:
            return match.group(0) # Return the full WhatsApp link
        # Also check page source for the link if redirect didn't change URL but loaded content
        page_source_match = WHATSAPP_LINK_PATTERN.search(driver.page_source)
        if page_source_match:
             status_log.info(f"Selenium: Found WhatsApp link in page source of {final_url}")
             return page_source_match.group(0)

    except Exception as e:
        status_log.error(f"Selenium: Error processing {page_url}: {e}")
    return None

async def run_sitemap_scraper_with_selenium(
    sitemap_url_input, 
    request_delay_sitemap, # For fetching sitemap URLs
    selenium_wait_time, 
    join_button_selector,
    progress_bar_ref, status_text_ref, detailed_log_area_ref
):
    all_found_whatsapp_links = set()
    pages_processed_count = 0
    processed_page_results = [] # (url, status, found_link)

    # 1. Fetch all page URLs from sitemap using aiohttp (fast)
    headers_http = {'User-Agent': USER_AGENT_HTTP}
    async with aiohttp.ClientSession(headers=headers_http) as session:
        if detailed_log_area_ref: detailed_log_area_ref.info(f"Fetching page URLs from: {sitemap_url_input}")
        page_urls_to_scrape = await fetch_all_page_urls_from_sitemaps_recursive_bs(
            sitemap_url_input, session, request_delay_sitemap, headers_http, detailed_log_area_ref
        )

    if not page_urls_to_scrape:
        status_text_ref.error("No page URLs found from the sitemap(s).")
        if progress_bar_ref: progress_bar_ref.progress(1.0)
        return set(), 0, []

    total_urls = len(page_urls_to_scrape)
    status_text_ref.info(f"Found {total_urls} page URLs. Initializing Selenium for scraping...")
    if progress_bar_ref: progress_bar_ref.progress(0.0)

    # 2. Initialize Selenium Driver (once, if possible, or manage its lifecycle carefully)
    # For simplicity here, we'll re-init if it fails, but ideally, manage one driver.
    # Running many Selenium instances can be heavy. This will process sequentially.
    driver = setup_selenium_driver()
    if not driver:
        status_text_ref.error("Selenium WebDriver could not be initialized. Aborting.")
        return set(), 0, []

    try:
        for i, page_url in enumerate(page_urls_to_scrape):
            status_text_ref.info(f"Processing URL {i+1}/{total_urls}: {page_url}")
            
            whatsapp_link = scrape_page_with_selenium(
                driver, page_url, selenium_wait_time, join_button_selector, detailed_log_area_ref
            )
            pages_processed_count += 1

            if whatsapp_link:
                all_found_whatsapp_links.add(whatsapp_link)
                processed_page_results.append((page_url, "Success", whatsapp_link))
                detailed_log_area_ref.success(f"Found: {whatsapp_link} on {page_url}")
            else:
                processed_page_results.append((page_url, "No WhatsApp link found after JS wait", None))
                detailed_log_area_ref.warning(f"No link found for: {page_url}")

            if progress_bar_ref: progress_bar_ref.progress((i + 1) / total_urls)
            
            # Brief pause between pages to be slightly gentler
            time.sleep(0.5) # Small delay between processing each URL with Selenium

    except Exception as e:
        status_text_ref.error(f"An error occurred during Selenium processing: {e}")
        detailed_log_area_ref.error(f"Critical Selenium error: {e}")
    finally:
        if driver:
            driver.quit()
            status_text_ref.info("Selenium WebDriver closed.")

    if progress_bar_ref: progress_bar_ref.progress(1.0)
    return all_found_whatsapp_links, pages_processed_count, processed_page_results

# --- Streamlit App UI ---
st.set_page_config(page_title="Advanced WhatsApp Link Extractor", layout="wide")
st.title("🔗 Advanced WhatsApp Link Extractor (Sitemap + JS Timer)")
st.markdown("""
This app fetches URLs from a sitemap and then uses **Selenium** to visit each page,
wait for potential JavaScript timers/redirects, and extract WhatsApp group links.

**IMPORTANT:**
- This version uses Selenium, which requires a browser (e.g., Chrome) and its driver (ChromeDriver) to be installed.
- It's designed primarily for **local execution**. Deployment on Streamlit Cloud free tier is challenging due to Selenium's dependencies.
- The "Join Button Selector" is crucial if navigation to a timer page is needed. Start with button text (e.g., "Join Group"). For complex cases, CSS selectors or XPath might be needed (advanced).
""")

st.sidebar.header("Sitemap & Fetching Settings")
sitemap_url_input = st.sidebar.text_input(
    "Enter Sitemap URL:",
    placeholder="e.g., https://example.com/sitemap.xml"
)
request_delay_sitemap_fetch = st.sidebar.slider(
    "Delay for Sitemap Fetch (s):", 0.0, 2.0, 0.1, 0.05,
    help="Delay between requests when fetching sitemap files themselves."
)

st.sidebar.header("Selenium Settings")
selenium_wait_time = st.sidebar.slider(
    "JS Timer Wait (seconds):", 1, 15, 5,
    help="How long to wait on a page for JavaScript timers/redirects to complete. Your site needs ~3s."
)
join_button_text_css_xpath = st.sidebar.text_input(
    "Join Button Text / CSS Selector / XPath:",
    placeholder="e.g., Join Group OR #joinButtonId OR //button[contains(text(),'Join')]",
    help="Text of the button to click to reach the timer page. Or a CSS/XPath selector. Leave blank if direct page has timer."
)


# Session state
if 'adv_scraping_done' not in st.session_state:
    st.session_state.adv_scraping_done = False
# ... (initialize other session state vars if needed)


if st.button("🚀 Start Advanced Scraping", type="primary"):
    if not sitemap_url_input:
        st.error("⚠️ Please enter a Sitemap URL.")
    elif not is_valid_url(sitemap_url_input):
        st.error("⚠️ Please enter a valid URL for the sitemap.")
    else:
        st.session_state.adv_scraping_done = False
        st.session_state.adv_found_links = []
        st.session_state.adv_pages_scraped = 0
        st.session_state.adv_processed_results = []

        progress_bar_placeholder = st.empty()
        status_text_placeholder = st.empty()
        detailed_log_expander = st.expander("Detailed Processing Log (Selenium)", expanded=True)
        detailed_log_area = detailed_log_expander.empty()

        progress_bar = progress_bar_placeholder.progress(0)
        status_text_placeholder.info("🚀 Initializing advanced scraper...")

        try:
            # Selenium part is synchronous, but sitemap fetching can be async
            # For simplicity in integrating, we'll run the main orchestrator that uses async for sitemap
            # and then sync for selenium processing.
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            # Note: run_sitemap_scraper_with_selenium is not fully async itself because of Selenium.
            # It uses asyncio only for the initial sitemap URL fetching.
            # A more complex setup would use threads for Selenium tasks if true parallel Selenium was desired.
            # For now, Selenium will process URLs one by one.
            
            # Directly call the orchestrator, which will handle its async part
            found_links_set, pages_scraped, processed_results = loop.run_until_complete(
                 run_sitemap_scraper_with_selenium(
                    sitemap_url_input,
                    request_delay_sitemap_fetch,
                    selenium_wait_time,
                    join_button_text_css_xpath,
                    progress_bar,
                    status_text_placeholder,
                    detailed_log_area
                )
            )
            
            st.session_state.adv_found_links = sorted(list(found_links_set))
            st.session_state.adv_pages_scraped = pages_scraped
            st.session_state.adv_processed_results = processed_results
            st.session_state.adv_scraping_done = True

            if st.session_state.adv_found_links:
                status_text_placeholder.success(
                    f"✅ Advanced scraping complete! Found {len(st.session_state.adv_found_links)} unique WhatsApp links "
                    f"from {st.session_state.adv_pages_scraped} pages processed using Selenium."
                )
            else:
                status_text_placeholder.info(
                    f"ℹ️ Advanced scraping complete. No WhatsApp links found after processing "
                    f"{st.session_state.adv_pages_scraped} pages with Selenium."
                )
            progress_bar_placeholder.empty()

        except Exception as e:
            st.error(f"An unexpected error occurred during advanced scraping: {e}")
            import traceback
            detailed_log_area.error(traceback.format_exc())
            status_text_placeholder.error(f"Scraping failed: {e}")
            if progress_bar_placeholder: progress_bar_placeholder.empty()


# Display results
if st.session_state.get('adv_scraping_done', False): # Use .get for safety
    st.subheader(f"📊 Selenium Results: {len(st.session_state.adv_found_links)} links from {st.session_state.adv_pages_scraped} pages")
    if st.session_state.adv_found_links:
        df_links = pd.DataFrame(st.session_state.adv_found_links, columns=["WhatsApp Group Link (via Selenium)"])
        st.dataframe(df_links, use_container_width=True)
        # ... (CSV download button)
    else:
        st.info("No WhatsApp links found by Selenium.")
    
    if st.session_state.adv_processed_results:
        with st.expander("Show Detailed Selenium Page Processing Summary"):
            df_summary = pd.DataFrame(st.session_state.adv_processed_results, columns=["Page URL", "Selenium Status", "Found Link"])
            st.dataframe(df_summary, use_container_width=True)

st.markdown("---")
st.markdown("Selenium-based scraper. Remember to respect website terms.")
