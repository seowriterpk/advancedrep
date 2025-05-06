import streamlit as st
import requests # Still used for initial synchronous checks if any, or fallback
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import pandas as pd
import re
import time
from collections import deque
import asyncio
import aiohttp

# --- Constants and Configuration ---
USER_AGENT = "WhatsAppLinkExtractor/2.1 (StreamlitApp; +https://github.com/yourusername/whatsapp-link-extractor-sitemap)" # Updated version
WHATSAPP_INVITE_CODE_PATTERN = re.compile(r"https?://chat\.whatsapp\.com/([A-Za-z0-9\-_]+)")

# --- Helper Functions ---
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

def find_whatsapp_invite_codes(page_content, pattern):
    return set(pattern.findall(page_content))

# --- Asynchronous Network and Scraping Functions ---
async def fetch_content_async(session, url, delay, headers, log_area): # Changed param name
    await asyncio.sleep(delay)
    try:
        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=20)) as response:
            response.raise_for_status()
            return await response.text(), response.headers.get('content-type', '').lower()
    except asyncio.TimeoutError:
        if log_area: log_area.warning(f"Timeout fetching: {url}") # Call directly
        return None, None
    except aiohttp.ClientError as e:
        if log_area: log_area.warning(f"HTTP Error for {url}: {e}") # Call directly
        return None, None
    except Exception as e:
        if log_area: log_area.warning(f"Generic Error for {url}: {e}") # Call directly
        return None, None

async def get_urls_from_sitemap_xml(sitemap_xml_content, log_area): # Changed param name
    page_urls = set()
    sub_sitemap_urls = set()
    try:
        soup = BeautifulSoup(sitemap_xml_content, 'xml')
        sitemap_tags = soup.find_all('sitemap')
        for sitemap_tag in sitemap_tags:
            loc_tag = sitemap_tag.find('loc')
            if loc_tag and loc_tag.string:
                sub_sitemap_urls.add(loc_tag.string.strip())

        url_tags = soup.find_all('url')
        for url_tag in url_tags:
            loc_tag = url_tag.find('loc')
            if loc_tag and loc_tag.string:
                page_url = loc_tag.string.strip()
                if is_valid_url(page_url):
                    page_urls.add(page_url)
        
        if log_area: # Check if log_area is provided
            if sub_sitemap_urls and page_urls:
                log_area.info(f"Sitemap contains both page URLs ({len(page_urls)}) and sub-sitemaps ({len(sub_sitemap_urls)}).") # Call directly
            elif sub_sitemap_urls:
                log_area.info(f"Sitemap is an index. Found {len(sub_sitemap_urls)} sub-sitemaps.") # Call directly
            elif page_urls:
                log_area.info(f"Sitemap contains {len(page_urls)} page URLs.") # Call directly
            else:
                log_area.warning("Sitemap does not seem to contain page URLs or sub-sitemap links.") # Call directly

    except Exception as e:
        if log_area: log_area.error(f"Error parsing sitemap XML: {e}") # Call directly
    return page_urls, sub_sitemap_urls

async def fetch_all_page_urls_from_sitemaps_recursive(initial_sitemap_url, session, delay, headers, log_area): # Changed param name
    all_page_urls_found = set()
    sitemap_queue = deque([initial_sitemap_url])
    processed_sitemaps = set()

    while sitemap_queue:
        current_sitemap_url = sitemap_queue.popleft()
        if current_sitemap_url in processed_sitemaps:
            continue
        processed_sitemaps.add(current_sitemap_url)

        if log_area: log_area.info(f"Processing sitemap: {current_sitemap_url}") # Call directly
        sitemap_content, _ = await fetch_content_async(session, current_sitemap_url, delay, headers, log_area) # Pass log_area

        if sitemap_content:
            page_urls, sub_sitemap_urls = await get_urls_from_sitemap_xml(sitemap_content, log_area) # Pass log_area
            all_page_urls_found.update(page_urls)
            for sub_sitemap in sub_sitemap_urls:
                if sub_sitemap not in processed_sitemaps:
                    sitemap_queue.append(sub_sitemap)
        else:
            if log_area: log_area.warning(f"Could not fetch content for sitemap: {current_sitemap_url}") # Call directly

    return all_page_urls_found

async def scrape_single_page_async(session, page_url, delay, headers, pattern, log_area): # Changed param name
    page_content, content_type = await fetch_content_async(session, page_url, delay, headers, log_area) # Pass log_area
    if not page_content:
        return set(), page_url, "Fetch Failed"

    if 'text/html' not in (content_type or ''):
        # if log_area: log_area.info(f"Skipping non-HTML: {page_url} ({content_type})") # Optional detailed log
        return set(), page_url, f"Skipped (Non-HTML: {content_type})"

    invite_codes = find_whatsapp_invite_codes(page_content, pattern)
    return invite_codes, page_url, "Success"

async def run_sitemap_scraper(
    sitemap_url_input, max_concurrent, request_delay,
    progress_bar_ref, status_text_ref, detailed_log_area_ref # Keep this name for clarity from caller
):
    headers = {'User-Agent': USER_AGENT}
    all_found_whatsapp_links = set()
    pages_scraped_count = 0
    processed_page_results = []

    async with aiohttp.ClientSession(headers=headers) as session:
        if detailed_log_area_ref: detailed_log_area_ref.info(f"Fetching page URLs from: {sitemap_url_input}") # Direct call
        
        page_urls_to_scrape = await fetch_all_page_urls_from_sitemaps_recursive(
            sitemap_url_input, session, request_delay, headers, detailed_log_area_ref # Pass it as `log_area`
        )

        if not page_urls_to_scrape:
            status_text_ref.error("No page URLs found from the sitemap(s).")
            progress_bar_ref.progress(1.0)
            return set(), 0, []

        total_urls = len(page_urls_to_scrape)
        status_text_ref.info(f"Found {total_urls} unique page URLs. Starting scraping...")
        progress_bar_ref.progress(0.0)

        semaphore = asyncio.Semaphore(max_concurrent)
        tasks = []

        for page_url in page_urls_to_scrape:
            async def task_wrapper(url_to_scrape):
                async with semaphore:
                    return await scrape_single_page_async(
                        session, url_to_scrape, request_delay, headers,
                        WHATSAPP_INVITE_CODE_PATTERN, detailed_log_area_ref # Pass it as `log_area`
                    )
            tasks.append(task_wrapper(page_url))

        for i, future in enumerate(asyncio.as_completed(tasks)):
            invite_codes, processed_url, status = await future
            pages_scraped_count += 1
            num_links_on_page = 0
            if invite_codes:
                num_links_on_page = len(invite_codes)
                for code in invite_codes:
                    full_link = f"https://chat.whatsapp.com/{code}"
                    all_found_whatsapp_links.add(full_link)
            
            processed_page_results.append((processed_url, status, num_links_on_page))
            progress_bar_ref.progress(pages_scraped_count / total_urls)
            status_text_ref.info(
                f"[{pages_scraped_count}/{total_urls}] {status}: {processed_url} ({num_links_on_page} links)"
            )
            # If you want even more detailed per-page logs in the expander:
            # if detailed_log_area_ref:
            #     detailed_log_area_ref.text(f"Page {pages_scraped_count}/{total_urls} - {status}: {processed_url} ({num_links_on_page} links found)")


    status_text_ref.success(f"Scraping complete! Processed {pages_scraped_count} pages.")
    progress_bar_ref.progress(1.0)
    return all_found_whatsapp_links, pages_scraped_count, processed_page_results


# --- Streamlit App UI ---
st.set_page_config(page_title="Fast WhatsApp Link Extractor (Sitemap)", layout="wide")
st.title("🚀 Super-Fast WhatsApp Link Extractor (Sitemap Based)")
st.markdown("""
This app fetches all page URLs from a given sitemap (including nested sitemaps)
and then concurrently scrapes each page to find WhatsApp group join links.
""")

sitemap_url_input = st.text_input(
    "Enter Sitemap URL:",
    placeholder="e.g., https://example.com/sitemap.xml"
)
col1, col2 = st.columns(2)
with col1:
    max_concurrent_requests = st.slider(
        "Max Concurrent Requests:",
        min_value=1, max_value=50, value=10,
        help="Number of pages to fetch and process simultaneously. Higher values are faster but risk overloading the server or getting blocked."
    )
with col2:
    request_delay_seconds = st.slider(
        "Request Delay (seconds):",
        min_value=0.0, max_value=5.0, value=0.25, step=0.05,
        help="Delay before each request (even concurrent ones). Helps with politeness. Set to 0 for maximum speed (use responsibly)."
    )

if 'sitemap_scraping_done' not in st.session_state:
    st.session_state.sitemap_scraping_done = False
if 'sitemap_found_links' not in st.session_state:
    st.session_state.sitemap_found_links = []
if 'sitemap_pages_scraped' not in st.session_state:
    st.session_state.sitemap_pages_scraped = 0
if 'sitemap_processed_page_results' not in st.session_state:
    st.session_state.sitemap_processed_page_results = []


if st.button("🔗 Scrape WhatsApp Links from Sitemap", type="primary"):
    st.session_state.sitemap_scraping_done = False
    st.session_state.sitemap_found_links = []
    st.session_state.sitemap_pages_scraped = 0
    st.session_state.sitemap_processed_page_results = []

    if not sitemap_url_input:
        st.error("⚠️ Please enter a Sitemap URL.")
    elif not is_valid_url(sitemap_url_input):
        st.error("⚠️ Please enter a valid URL for the sitemap (e.g., http:// or https://).")
    else:
        progress_bar_placeholder = st.empty()
        status_text_placeholder = st.empty()
        detailed_log_expander = st.expander("Detailed Processing Log", expanded=False)
        detailed_log_area = detailed_log_expander.empty() # This is the DeltaGenerator

        progress_bar = progress_bar_placeholder.progress(0)
        status_text_placeholder.info("🚀 Initializing scraper...")

        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            found_links_set, pages_scraped, processed_results = loop.run_until_complete(
                run_sitemap_scraper(
                    sitemap_url_input,
                    max_concurrent_requests,
                    request_delay_seconds,
                    progress_bar,
                    status_text_placeholder,
                    detailed_log_area # Pass the DeltaGenerator directly
                )
            )
            st.session_state.sitemap_found_links = sorted(list(found_links_set))
            st.session_state.sitemap_pages_scraped = pages_scraped
            st.session_state.sitemap_processed_page_results = processed_results
            st.session_state.sitemap_scraping_done = True

            if st.session_state.sitemap_found_links:
                status_text_placeholder.success(
                    f"✅ Scraping complete! Found {len(st.session_state.sitemap_found_links)} unique WhatsApp links "
                    f"from {st.session_state.sitemap_pages_scraped} pages processed."
                )
            else:
                status_text_placeholder.info(
                    f"ℹ️ Scraping complete. No WhatsApp links found after processing "
                    f"{st.session_state.sitemap_pages_scraped} pages from the sitemap(s)."
                )
            progress_bar_placeholder.empty()

        except Exception as e:
            st.error(f"An unexpected error occurred during scraping: {e}")
            import traceback
            st.error(traceback.format_exc()) # Show full traceback for debugging
            status_text_placeholder.error(f"Scraping failed: {e}")
            if progress_bar_placeholder: progress_bar_placeholder.empty() # Ensure it's cleared on error


if st.session_state.sitemap_scraping_done:
    st.subheader(f"📊 Results: {len(st.session_state.sitemap_found_links)} unique links found from {st.session_state.sitemap_pages_scraped} pages")

    if st.session_state.sitemap_found_links:
        df_links = pd.DataFrame(st.session_state.sitemap_found_links, columns=["WhatsApp Group Link"])
        st.dataframe(df_links, use_container_width=True, height=300)

        csv_data = df_links.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="📥 Download Links as CSV",
            data=csv_data,
            file_name=f"whatsapp_links_sitemap_{get_domain(sitemap_url_input) or 'extracted'}.csv",
            mime='text/csv',
        )
    else:
        st.info("No WhatsApp group links were found on the processed pages.")

    if st.session_state.sitemap_processed_page_results:
        with st.expander("Show Detailed Page Processing Summary", expanded=False):
            df_processed_summary = pd.DataFrame(
                st.session_state.sitemap_processed_page_results,
                columns=["Page URL", "Status", "WhatsApp Links Found on Page"]
            )
            st.dataframe(df_processed_summary, use_container_width=True)

st.markdown("---")
st.markdown(
    "Built with [Streamlit](https://streamlit.io), `aiohttp`, and `BeautifulSoup`. "
    "Remember to use responsibly and respect website terms of service."
)
