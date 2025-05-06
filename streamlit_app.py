import streamlit as st
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import pandas as pd
import re
import time
from collections import deque
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from robotexclusionrulesparser import RobotExclusionRulesParser
import logging

# --- Configuration & Constants ---
APP_VERSION = "2.0"
DEFAULT_USER_AGENT = f"WhatsAppLinkExtractor/{APP_VERSION} (StreamlitApp; +https://your-repo-link-here)" # CHANGE THIS
REQUEST_TIMEOUT = 15  # seconds
THREAD_REQUEST_DELAY = 0.5 # seconds delay per thread request to be polite
MAX_RETRIES = 3
BACKOFF_FACTOR = 0.3

# Logging (optional, helpful for debugging)
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# logger = logging.getLogger(__name__)


# --- Helper Functions ---

def is_valid_url(url):
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc]) and result.scheme in ['http', 'https']
    except ValueError:
        return False

def get_domain(url):
    try:
        return urlparse(url).netloc
    except Exception:
        return None

def normalize_url(url):
    """Normalizes a URL by removing fragments and ensuring a consistent scheme."""
    parsed = urlparse(url)
    # Prefer https, ensure scheme is present
    scheme = parsed.scheme if parsed.scheme else 'http'
    # Remove fragment, trailing slash for consistency in visited set
    path = parsed.path.rstrip('/')
    return parsed._replace(scheme=scheme, path=path, params='', query='', fragment='').geturl()

def find_whatsapp_links_on_page(page_content):
    pattern = r"https?://chat\.whatsapp\.com/([A-Za-z0-9\-_]{15,})" # More specific invite code length
    return set(f"https://chat.whatsapp.com/{match}" for match in re.findall(pattern, page_content))


# --- Robots.txt Handling ---
def fetch_robots_txt(session, base_url):
    robots_url = urljoin(base_url, "/robots.txt")
    try:
        response = session.get(robots_url, timeout=REQUEST_TIMEOUT / 2) # Shorter timeout for robots.txt
        response.raise_for_status()
        parser = RobotExclusionRulesParser()
        parser.parse(response.text)
        return parser
    except requests.RequestException as e:
        st.sidebar.warning(f"Could not fetch or parse robots.txt from {robots_url}: {e}. Assuming allow all.")
        return None
    except Exception as e:
        st.sidebar.warning(f"Error processing robots.txt: {e}. Assuming allow all.")
        return None

def is_allowed_by_robots(robots_parser, url, user_agent):
    if robots_parser is None:
        return True # If no robots.txt, assume allowed
    try:
        return robots_parser.is_allowed(user_agent, url)
    except Exception as e: # Catch potential errors in the parser library itself for malformed robots.txt
        # logger.error(f"Error checking robots.txt for {url}: {e}")
        return True # Fail open


# --- Core Crawler Logic ---

class WebCrawler:
    def __init__(self, start_url, max_depth, max_pages, num_threads, user_agent, progress_callback):
        self.start_url = normalize_url(start_url)
        self.base_domain = get_domain(self.start_url)
        self.max_depth = max_depth
        self.max_pages_to_crawl = max_pages
        self.num_threads = num_threads
        self.user_agent = user_agent
        self.progress_callback = progress_callback # Function to update Streamlit UI

        self.visited_urls = set()
        self.found_whatsapp_links = set()
        self.crawl_queue = deque([(self.start_url, 0)]) # (url, depth)
        self.pages_crawled_count = 0
        self.lock = threading.Lock() # To protect shared resources

        self.session = self._create_session()
        self.robots_parser = fetch_robots_txt(self.session, self.start_url)

    def _create_session(self):
        session = requests.Session()
        session.headers.update({'User-Agent': self.user_agent})
        retries = Retry(total=MAX_RETRIES, backoff_factor=BACKOFF_FACTOR,
                        status_forcelist=[500, 502, 503, 504])
        session.mount('http://', HTTPAdapter(max_retries=retries))
        session.mount('https://', HTTPAdapter(max_retries=retries))
        return session

    def _process_page(self, current_url, current_depth):
        with self.lock:
            if current_url in self.visited_urls or self.pages_crawled_count >= self.max_pages_to_crawl:
                return [], [] # Return empty lists if already visited or page limit reached

            if not is_allowed_by_robots(self.robots_parser, current_url, self.user_agent):
                # logger.info(f"Skipping (disallowed by robots.txt): {current_url}")
                self.visited_urls.add(current_url) # Add to visited to prevent re-queueing
                return [], []

            self.visited_urls.add(current_url)
            self.pages_crawled_count += 1
            page_num = self.pages_crawled_count # Capture current count for this page

        # logger.info(f"Thread {threading.get_ident()}: Crawling (Depth {current_depth}, Page {page_num}/{self.max_pages_to_crawl}): {current_url}")
        self.progress_callback(
            f"Crawling D{current_depth} P{page_num}/{self.max_pages_to_crawl}: {current_url}",
            page_num / self.max_pages_to_crawl if self.max_pages_to_crawl > 0 else 0
        )

        new_internal_links_to_queue = []
        page_whatsapp_links = set()

        try:
            time.sleep(THREAD_REQUEST_DELAY) # Polite delay
            response = self.session.get(current_url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()

            content_type = response.headers.get('content-type', '').lower()
            if 'text/html' not in content_type:
                # logger.warning(f"Skipping non-HTML content at {current_url} (type: {content_type})")
                return [], [] # Return empty lists for non-HTML content

            soup = BeautifulSoup(response.text, 'html.parser')
            page_whatsapp_links = find_whatsapp_links_on_page(response.text)

            if current_depth < self.max_depth:
                for link_tag in soup.find_all('a', href=True):
                    href = link_tag['href']
                    absolute_link = urljoin(current_url, href)
                    normalized_link = normalize_url(absolute_link)

                    if (is_valid_url(normalized_link) and
                        get_domain(normalized_link) == self.base_domain and
                        normalized_link not in self.visited_urls): # Check visited again before adding to queue
                        new_internal_links_to_queue.append((normalized_link, current_depth + 1))

        except requests.exceptions.RequestException as e:
            self.progress_callback(f"Failed: {current_url.split('//')[-1]} - {type(e).__name__}", None, is_error=True)
            # logger.error(f"Request failed for {current_url}: {e}")
        except Exception as e:
            self.progress_callback(f"Error: {current_url.split('//')[-1]} - {type(e).__name__}", None, is_error=True)
            # logger.error(f"Error processing {current_url}: {e}")

        return page_whatsapp_links, new_internal_links_to_queue


    def crawl(self):
        active_tasks = 0
        with ThreadPoolExecutor(max_workers=self.num_threads) as executor:
            futures = {} # Store future -> (url, depth)

            # Submit initial URL(s)
            if self.crawl_queue:
                url, depth = self.crawl_queue.popleft()
                if url not in self.visited_urls: # Check before submitting
                    future = executor.submit(self._process_page, url, depth)
                    futures[future] = (url, depth)
                    active_tasks +=1

            while futures: # Process as long as there are active or pending tasks
                if self.pages_crawled_count >= self.max_pages_to_crawl and self.max_pages_to_crawl > 0:
                    # logger.info(f"Max pages limit ({self.max_pages_to_crawl}) reached. Shutting down executor.")
                    # executor.shutdown(wait=True, cancel_futures=True) # Python 3.9+ for cancel_futures
                    # For wider compatibility, just stop submitting new tasks and let current ones finish
                    for fut in list(futures.keys()): # Iterate over a copy for modification
                        if not fut.done():
                            fut.cancel() # Attempt to cancel
                        if fut in futures: # Check if still there (might have completed quickly)
                           del futures[fut]
                    break # Exit the while loop

                completed_futures = []
                for future in as_completed(futures.keys(), timeout=1): # Timeout to allow checking limits
                    active_tasks -=1
                    page_wa_links, new_links_to_queue = future.result()
                    completed_futures.append(future) # Mark for removal

                    with self.lock:
                        self.found_whatsapp_links.update(page_wa_links)

                    # Add new links to the queue and submit them if within limits
                    for new_url, new_depth in new_links_to_queue:
                        with self.lock: # Ensure thread-safe check and addition
                            can_add_to_queue = (
                                new_url not in self.visited_urls and
                                self.pages_crawled_count + active_tasks < self.max_pages_to_crawl and # Approximation
                                new_depth <= self.max_depth
                            )
                            if can_add_to_queue:
                                # Add to visited here to prevent duplicate submissions from multiple threads
                                # finding the same link before it's processed.
                                self.visited_urls.add(new_url) # Pre-mark as visited to avoid race conditions
                                self.crawl_queue.append((new_url,new_depth))


                for fut in completed_futures:
                    if fut in futures:
                        del futures[fut] # Remove completed/cancelled future

                # Submit new tasks from the queue
                while self.crawl_queue and active_tasks < self.num_threads and \
                      (self.pages_crawled_count + active_tasks < self.max_pages_to_crawl or self.max_pages_to_crawl == 0):
                    url_to_process, depth_to_process = self.crawl_queue.popleft()
                    # Final check for visited (might have been added by another thread just before pre-marking)
                    # This specific pre-marking in the previous loop should reduce this need, but double check is safer.
                    # if url_to_process not in self.visited_urls: # Covered by pre-marking
                    future = executor.submit(self._process_page, url_to_process, depth_to_process)
                    futures[future] = (url_to_process, depth_to_process)
                    active_tasks +=1

                if not self.crawl_queue and not futures: # No more tasks to submit and no active tasks
                    break

        self.progress_callback("Crawl finished.", 1.0 if self.max_pages_to_crawl > 0 else 0.0)
        return self.found_whatsapp_links, self.pages_crawled_count


# --- Streamlit App UI ---
st.set_page_config(page_title="Advanced WhatsApp Link Extractor", layout="wide")
st.title(f"🔗 Advanced WhatsApp Group Link Extractor (v{APP_VERSION})")
st.markdown("""
This app concurrently crawls a website within the same domain to extract WhatsApp group join links.
It respects `robots.txt`, uses multiple threads for speed, and includes retry mechanisms.
""")

# --- Session State Initialization ---
if 'crawling_in_progress' not in st.session_state:
    st.session_state.crawling_in_progress = False
if 'found_links_df' not in st.session_state:
    st.session_state.found_links_df = pd.DataFrame(columns=["WhatsApp Group Link"])
if 'crawl_stats' not in st.session_state:
    st.session_state.crawl_stats = {"pages_crawled": 0, "links_found": 0, "start_url": ""}

# --- Input Form ---
with st.sidebar:
    st.header("⚙️ Crawl Settings")
    start_url_input = st.text_input(
        "Enter Starting URL:",
        placeholder="e.g., https://example.com",
        value=st.session_state.crawl_stats.get("start_url", "")
    )
    depth_input = st.slider(
        "Max Crawling Depth:",
        min_value=0, max_value=10, value=2,
        help="0 for start page only. How many 'clicks' deep to crawl."
    )
    max_pages_input = st.number_input(
        "Max Pages to Crawl (0 for no limit by page count):",
        min_value=0, max_value=5000, value=100, step=50, # Sensible default & max for Streamlit Cloud
        help="Stops crawling after this many pages, or max depth, whichever comes first."
    )
    num_threads_input = st.slider(
        "Number of Worker Threads:",
        min_value=1, max_value=10, value=4, # Max 10 to be reasonable on shared resources
        help="More threads can speed up crawling but increase server load."
    )
    user_agent_input = st.text_input(
        "Custom User-Agent (optional):",
        placeholder=DEFAULT_USER_AGENT,
        value=DEFAULT_USER_AGENT,
        help="Identify your bot. Leave as default or customize."
    )

    start_button = st.button("🚀 Start Crawling", type="primary", disabled=st.session_state.crawling_in_progress)
    if st.session_state.crawling_in_progress:
        st.warning("⚠️ Crawling is already in progress...")


# --- Progress and Status Area ---
status_placeholder = st.empty()
progress_bar_placeholder = st.empty()
# For detailed per-thread messages (optional, can be verbose)
# log_messages_placeholder = st.expander("Live Crawl Log", expanded=False)
# log_text_area = log_messages_placeholder.empty()
# live_log_messages = []


def update_streamlit_progress(message, progress_value, is_error=False):
    if progress_value is not None: # Can be None if just a message
        progress_bar_placeholder.progress(progress_value)

    if is_error:
        status_placeholder.error(message) # Show errors prominently
    else:
        status_placeholder.info(message)

    # Optional: for live log display
    # if message:
    #     live_log_messages.append(f"[{time.strftime('%H:%M:%S')}] {message}")
    #     if len(live_log_messages) > 20: # Keep log manageable
    #         live_log_messages.pop(0)
    #     log_text_area.text_area("", "".join(m + "\n" for m in reversed(live_log_messages)), height=200, disabled=True)


# --- Crawling Execution ---
if start_button:
    if not start_url_input:
        st.error("⚠️ Please enter a starting URL.")
    elif not is_valid_url(start_url_input):
        st.error(f"⚠️ Invalid URL: '{start_url_input}'. Must include http:// or https://.")
    else:
        st.session_state.crawling_in_progress = True
        st.session_state.found_links_df = pd.DataFrame(columns=["WhatsApp Group Link"]) # Reset
        # live_log_messages.clear()

        status_placeholder.info("🚀 Initializing crawl...")
        progress_bar_placeholder.progress(0)

        crawler = WebCrawler(
            start_url=start_url_input,
            max_depth=depth_input,
            max_pages=max_pages_input if max_pages_input > 0 else float('inf'), # Use infinity if 0
            num_threads=num_threads_input,
            user_agent=user_agent_input or DEFAULT_USER_AGENT,
            progress_callback=update_streamlit_progress
        )

        try:
            with st.spinner(f"🔍 Crawling {get_domain(start_url_input)} with up to {num_threads_input} threads..."):
                found_links_set, pages_crawled = crawler.crawl()

            st.session_state.crawl_stats = {
                "pages_crawled": pages_crawled,
                "links_found": len(found_links_set),
                "start_url": start_url_input # Store for next run
            }
            if found_links_set:
                st.session_state.found_links_df = pd.DataFrame(sorted(list(found_links_set)), columns=["WhatsApp Group Link"])
                status_placeholder.success(f"✅ Crawl complete! Found {st.session_state.crawl_stats['links_found']} WhatsApp links across {st.session_state.crawl_stats['pages_crawled']} pages.")
            else:
                status_placeholder.info(f"ℹ️ Crawl complete. No WhatsApp links found after checking {st.session_state.crawl_stats['pages_crawled']} pages.")
            progress_bar_placeholder.progress(1.0)

        except Exception as e:
            status_placeholder.error(f"🚨 An unexpected error occurred during crawling: {e}")
            # logger.exception("Unhandled exception in crawl execution:")
        finally:
            st.session_state.crawling_in_progress = False
# NEW
st.rerun() # To re-enable button and update UI correctly
# --- Display Results ---
st.subheader("📊 Results")
if not st.session_state.found_links_df.empty:
    st.metric("Pages Scanned", st.session_state.crawl_stats.get("pages_crawled", 0))
    st.metric("WhatsApp Links Found", st.session_state.crawl_stats.get("links_found", 0))

    st.dataframe(st.session_state.found_links_df, use_container_width=True)

    csv_data = st.session_state.found_links_df.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="📥 Download Links as CSV",
        data=csv_data,
        file_name=f"whatsapp_links_{get_domain(st.session_state.crawl_stats.get('start_url')) or 'extracted'}.csv",
        mime='text/csv',
    )
elif st.session_state.crawl_stats.get("pages_crawled", 0) > 0 : # Crawled but found nothing
     st.info("No WhatsApp group links were found in the crawled pages.")
else: # Before any crawl or if crawl was invalid
     st.info("Enter settings and start crawling to see results.")


st.markdown("---")
st.markdown(f"Advanced WhatsApp Link Extractor v{APP_VERSION} | "
            "Remember to be an ethical netizen: respect website terms, `robots.txt`, and avoid overloading servers.")
st.caption("This tool uses multi-threading and respects robots.txt. It does not execute JavaScript.")
