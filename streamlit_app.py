import streamlit as st
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import pandas as pd
import re
import time
from collections import deque
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import queue # Thread-safe queue

# --- Constants ---
USER_AGENT = "WhatsAppLinkExtractor/2.0 (StreamlitApp; +https://your-github-repo)" # BE A GOOD BOT CITIZEN! Update this.
DEFAULT_REQUEST_DELAY = 0.5  # Seconds between requests PER WORKER
CONNECT_TIMEOUT = 10 # Seconds to wait for server connection
READ_TIMEOUT = 15    # Seconds to wait for server response

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

def find_whatsapp_links_on_page(page_content):
    pattern = r"https?://chat\.whatsapp\.com/([A-Za-z0-9\-_]+)"
    # Return full URLs directly
    return set(f"https://chat.whatsapp.com/{match}" for match in re.findall(pattern, page_content))


# --- Crawler Worker Function ---
def worker_task(url_to_crawl, current_depth, start_domain, visited_urls_lock, visited_urls_shared,
                task_queue_shared, results_queue_shared, new_links_to_process_queue,
                max_depth_crawl, stop_event, request_delay_worker):
    """
    Worker function to fetch and process a single URL.
    Puts results (WhatsApp links, new internal URLs) into respective queues.
    """
    if stop_event.is_set():
        return None, [], 0 # Signal to stop

    # Construct a unique key for visited_urls (URL without fragment)
    parsed_url_key = urlparse(url_to_crawl)
    url_key_for_visited = parsed_url_key._replace(fragment="").geturl()

    with visited_urls_lock:
        if url_key_for_visited in visited_urls_shared:
            return None, [], 0 # Already visited or added to queue
        visited_urls_shared.add(url_key_for_visited)

    thread_id = threading.get_ident()
    # st.write(f"[Thread {thread_id}] Crawling (Depth {current_depth}): {url_to_crawl}") # Too noisy for main UI

    try:
        time.sleep(request_delay_worker) # Politeness delay per worker
        headers = {'User-Agent': USER_AGENT}
        response = requests.get(url_to_crawl, headers=headers, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
        response.raise_for_status()

        content_type = response.headers.get('content-type', '').lower()
        if 'text/html' not in content_type:
            # st.write(f"[Thread {thread_id}] Skipping non-HTML: {url_to_crawl}")
            return url_to_crawl, [], 1 # Page processed (attempted)

        soup = BeautifulSoup(response.text, 'html.parser')
        page_whatsapp_links = find_whatsapp_links_on_page(response.text)
        for link in page_whatsapp_links:
            results_queue_shared.put(link)

        found_internal_links = []
        if current_depth < max_depth_crawl:
            for link_tag in soup.find_all('a', href=True):
                href = link_tag['href']
                absolute_link = urljoin(url_to_crawl, href)
                parsed_link = urlparse(absolute_link)
                clean_link = parsed_link._replace(fragment="").geturl() # Remove fragment for queue

                if is_valid_url(clean_link) and get_domain(clean_link) == start_domain:
                    # Check visited again before adding to prevent race condition additions
                    # The main check is at the start of this function, this is a secondary check
                    with visited_urls_lock:
                        if clean_link not in visited_urls_shared:
                             # We don't add to visited_urls_shared here, but at the start of worker_task
                             # when it's popped from the queue.
                            found_internal_links.append((clean_link, current_depth + 1))

        # Instead of directly adding to task_queue_shared from worker,
        # put them in a temporary queue for the main thread to manage.
        # This simplifies task_queue_shared management and makes progress tracking easier.
        if found_internal_links:
            new_links_to_process_queue.put(found_internal_links)

        return url_to_crawl, list(page_whatsapp_links), 1 # URL, its WhatsApp links, 1 page processed

    except requests.exceptions.Timeout:
        # st.write(f"[Thread {thread_id}] Timeout: {url_to_crawl}")
        st.session_state.error_messages.append(f"Timeout fetching: {url_to_crawl}")
        return url_to_crawl, [], 1 # Still counts as processed (attempted)
    except requests.exceptions.RequestException as e:
        # st.write(f"[Thread {thread_id}] Request Error {url_to_crawl}: {e}")
        st.session_state.error_messages.append(f"Failed: {url_to_crawl} ({type(e).__name__})")
        return url_to_crawl, [], 1 # Still counts as processed (attempted)
    except Exception as e:
        # st.write(f"[Thread {thread_id}] Processing Error {url_to_crawl}: {e}")
        st.session_state.error_messages.append(f"Error processing: {url_to_crawl} ({type(e).__name__})")
        return url_to_crawl, [], 1 # Still counts as processed (attempted)

# --- Streamlit App UI ---
st.set_page_config(page_title="Fast WhatsApp Link Extractor", layout="wide")
st.title("🚀 Fast WhatsApp Group Link Extractor (Concurrent)")
st.markdown("""
This app attempts to quickly scan a website using concurrent workers to extract WhatsApp group join links.
**Use responsibly! High concurrency can strain servers.**
Adjust concurrency, crawling depth, and view live progress.
""")

# --- Initialize Session State ---
if 'crawling_in_progress' not in st.session_state:
    st.session_state.crawling_in_progress = False
if 'found_whatsapp_links_set' not in st.session_state: # Store unique links
    st.session_state.found_whatsapp_links_set = set()
if 'displayed_whatsapp_links' not in st.session_state: # For live display
    st.session_state.displayed_whatsapp_links = []
if 'pages_crawled_count' not in st.session_state:
    st.session_state.pages_crawled_count = 0
if 'urls_in_queue_count' not in st.session_state:
    st.session_state.urls_in_queue_count = 0
if 'start_url_processed' not in st.session_state:
    st.session_state.start_url_processed = ""
if 'error_messages' not in st.session_state:
    st.session_state.error_messages = []
if 'stop_event' not in st.session_state:
    st.session_state.stop_event = threading.Event()


# --- Input Fields ---
with st.sidebar:
    st.header("⚙️ Crawler Settings")
    start_url_input = st.text_input(
        "Enter Starting URL:",
        placeholder="e.g., https://example.com",
        value=st.session_state.get('last_start_url', "")
    )
    max_depth_input = st.slider(
        "Crawling Depth (0 for start page):", 0, 10, 1,
        help="Max link 'clicks' from start. Higher values increase time/pages significantly."
    )
    num_workers_input = st.slider(
        "Number of Concurrent Workers:", 1, 10, 3,
        help="More workers = faster, but more server load. Be cautious!"
    )
    request_delay_input = st.number_input(
        "Delay per Request (seconds):", min_value=0.1, max_value=5.0, value=DEFAULT_REQUEST_DELAY, step=0.1,
        help="Politeness delay for each worker's request."
    )

col_start, col_stop = st.columns(2)
with col_start:
    if st.button("🚀 Start Crawling", type="primary", disabled=st.session_state.crawling_in_progress, use_container_width=True):
        if not start_url_input:
            st.error("⚠️ Please enter a starting URL.")
        elif not is_valid_url(start_url_input):
            st.error("⚠️ Please enter a valid URL (e.g., http:// or https://).")
        else:
            # Reset state for a new crawl
            st.session_state.crawling_in_progress = True
            st.session_state.found_whatsapp_links_set = set()
            st.session_state.displayed_whatsapp_links = []
            st.session_state.pages_crawled_count = 0
            st.session_state.urls_in_queue_count = 0
            st.session_state.start_url_processed = start_url_input
            st.session_state.last_start_url = start_url_input # Remember for next time
            st.session_state.error_messages = []
            st.session_state.stop_event.clear()

            # Initialize queues and shared structures for this crawl
            st.session_state.task_queue = deque([(start_url_input, 0)])
            st.session_state.results_queue = queue.Queue() # For WhatsApp links
            st.session_state.new_links_to_process_queue = queue.Queue() # For new internal URLs from workers
            st.session_state.visited_urls = set() # URLs added to task_queue or processed
            st.session_state.visited_urls_lock = threading.Lock()

            # Add initial URL to visited (it's in task_queue)
            with st.session_state.visited_urls_lock:
                 parsed_initial = urlparse(start_url_input)
                 st.session_state.visited_urls.add(parsed_initial._replace(fragment="").geturl())

            st.session_state.urls_in_queue_count = 1
            st.info(f"🚀 Crawl started for {start_url_input} with {num_workers_input} workers, depth {max_depth_input}.")

with col_stop:
    if st.button("🛑 Stop Crawling", disabled=not st.session_state.crawling_in_progress, use_container_width=True):
        st.session_state.stop_event.set()
        st.warning("🛑 Stop signal sent. Workers will finish current tasks and stop.")
        # The main loop will eventually see crawling_in_progress become False


# --- Live Status Placeholders ---
status_col1, status_col2, status_col3 = st.columns(3)
pages_crawled_placeholder = status_col1.empty()
links_found_placeholder = status_col2.empty()
queue_size_placeholder = status_col3.empty()
progress_bar_placeholder = st.empty()
live_links_header_placeholder = st.empty()
live_links_display_placeholder = st.empty() # For DataFrame
error_messages_placeholder = st.empty()

# --- Main Crawling Loop (if active) ---
if st.session_state.crawling_in_progress:
    start_domain_crawl = get_domain(st.session_state.start_url_processed)

    # Check if we need to start the ThreadPoolExecutor
    if 'executor' not in st.session_state or st.session_state.executor is None:
        st.session_state.executor = ThreadPoolExecutor(max_workers=num_workers_input)
        st.session_state.futures = [] # To keep track of submitted tasks

    # Submit initial tasks from task_queue if futures list is empty
    # and executor exists
    if st.session_state.executor and not st.session_state.futures:
        with st.session_state.visited_urls_lock: # Ensure task_queue is accessed safely if needed
            # Only submit new tasks if queue is not empty
            tasks_to_submit_count = min(len(st.session_state.task_queue), num_workers_input * 2) # Submit a batch
            for _ in range(tasks_to_submit_count):
                if not st.session_state.task_queue:
                    break
                url, depth = st.session_state.task_queue.popleft()
                # visited_urls_shared is st.session_state.visited_urls
                # task_queue_shared is st.session_state.task_queue
                # results_queue_shared is st.session_state.results_queue
                future = st.session_state.executor.submit(
                    worker_task, url, depth, start_domain_crawl,
                    st.session_state.visited_urls_lock,
                    st.session_state.visited_urls,
                    st.session_state.task_queue, # For potential direct additions (though we use intermediate queue)
                    st.session_state.results_queue,
                    st.session_state.new_links_to_process_queue,
                    max_depth_input,
                    st.session_state.stop_event,
                    request_delay_input
                )
                st.session_state.futures.append(future)

    # Process completed futures
    new_futures = []
    for future in st.session_state.futures:
        if future.done():
            if st.session_state.stop_event.is_set():
                continue # Skip processing if stopping

            try:
                processed_url, _ , pages_increment = future.result() # We get WA links via results_queue
                if pages_increment:
                    st.session_state.pages_crawled_count += pages_increment
            except Exception as e:
                st.session_state.error_messages.append(f"Worker error: {e}")
                st.session_state.pages_crawled_count += 1 # Count as attempted
        else:
            new_futures.append(future) # Keep non-done futures
    st.session_state.futures = new_futures

    # Process newly discovered internal links from workers
    while not st.session_state.new_links_to_process_queue.empty():
        links_batch = st.session_state.new_links_to_process_queue.get_nowait()
        with st.session_state.visited_urls_lock:
            for link_url, link_depth in links_batch:
                # Check visited_urls again here before adding to task_queue
                # url_key_for_visited logic should be applied before this check if not already
                parsed_new_link = urlparse(link_url)
                new_link_key = parsed_new_link._replace(fragment="").geturl()
                if new_link_key not in st.session_state.visited_urls:
                    st.session_state.visited_urls.add(new_link_key) # Mark as added to queue
                    st.session_state.task_queue.append((link_url, link_depth))


    # Replenish futures from task_queue if workers are available
    # and not stopping
    if st.session_state.executor and not st.session_state.stop_event.is_set():
        num_active_futures = len(st.session_state.futures)
        available_slots = num_workers_input - num_active_futures
        if available_slots > 0:
            with st.session_state.visited_urls_lock: # Protect task_queue access
                tasks_to_submit_count = min(len(st.session_state.task_queue), available_slots)
                for _ in range(tasks_to_submit_count):
                    if not st.session_state.task_queue:
                        break
                    url, depth = st.session_state.task_queue.popleft()
                    future = st.session_state.executor.submit(
                        worker_task, url, depth, start_domain_crawl,
                        st.session_state.visited_urls_lock,
                        st.session_state.visited_urls,
                        st.session_state.task_queue,
                        st.session_state.results_queue,
                        st.session_state.new_links_to_process_queue,
                        max_depth_input,
                        st.session_state.stop_event,
                        request_delay_input
                    )
                    st.session_state.futures.append(future)


    # Collect WhatsApp links from results_queue
    while not st.session_state.results_queue.empty():
        link = st.session_state.results_queue.get_nowait()
        if link not in st.session_state.found_whatsapp_links_set:
            st.session_state.found_whatsapp_links_set.add(link)
            st.session_state.displayed_whatsapp_links.append(link) # Add to list for display

    # Update live counters
    st.session_state.urls_in_queue_count = len(st.session_state.task_queue) + len(st.session_state.futures) # Approx

    pages_crawled_placeholder.metric("Pages Crawled/Attempted", st.session_state.pages_crawled_count)
    links_found_placeholder.metric("Unique WhatsApp Links", len(st.session_state.found_whatsapp_links_set))
    queue_size_placeholder.metric("URLs in Queue", st.session_state.urls_in_queue_count)

    # Progress Bar
    total_known_urls = len(st.session_state.visited_urls) # URLs that are or have been in queue
    if total_known_urls > 0 :
        progress_val = min(1.0, st.session_state.pages_crawled_count / total_known_urls if total_known_urls else 0)
        progress_bar_placeholder.progress(progress_val)
    else:
        progress_bar_placeholder.progress(0)


    # Live display of found links
    if st.session_state.displayed_whatsapp_links:
        live_links_header_placeholder.subheader(f"🔗 Live View: Found WhatsApp Links ({len(st.session_state.displayed_whatsapp_links)})")
        # Display as a list for simplicity with live updates
        # Using st.code or st.text for dynamic updates might be lighter than st.dataframe
        # For many links, st.dataframe is better but might be slower to update live.
        # Let's use a text area that can grow.
        links_text = "\n".join(st.session_state.displayed_whatsapp_links)
        live_links_display_placeholder.text_area("Links:", links_text, height=200, key="live_links_text_area")


    # Display error messages
    if st.session_state.error_messages:
        error_messages_placeholder.expander("⚠️ Encountered Errors/Warnings").warning("\n".join(st.session_state.error_messages[-20:])) # Show last 20

    # Check for crawl completion or stop signal
    if st.session_state.stop_event.is_set() or \
       (not st.session_state.task_queue and not st.session_state.futures):
        st.session_state.crawling_in_progress = False
        if st.session_state.executor:
            st.session_state.executor.shutdown(wait=True) # Wait for threads to finish
            st.session_state.executor = None
        st.session_state.futures = []

        if st.session_state.stop_event.is_set():
            st.success("✅ Crawl stopped by user. Processed results up to this point are shown.")
        else:
            st.success(f"✅ Crawl finished! Checked {st.session_state.pages_crawled_count} pages. Found {len(st.session_state.found_whatsapp_links_set)} unique WhatsApp links.")
        progress_bar_placeholder.progress(1.0) # Ensure it shows 100%
        # Clear live update placeholders after completion if desired, or leave them
        # live_links_header_placeholder.empty()
        # live_links_display_placeholder.empty()

    # Rerun periodically to update UI from thread activity
    if st.session_state.crawling_in_progress:
        time.sleep(0.5) # Adjust for responsiveness vs. CPU usage
        st.rerun()

# --- Display Final Results (after crawl stops or is complete) ---
if not st.session_state.crawling_in_progress and st.session_state.start_url_processed: # Only show if a crawl was run
    st.markdown("---")
    st.subheader(f"📊 Final Results for {st.session_state.start_url_processed}")
    st.write(f"Total pages crawled/attempted: {st.session_state.pages_crawled_count}")
    st.write(f"Total unique WhatsApp links found: {len(st.session_state.found_whatsapp_links_set)}")

    if st.session_state.found_whatsapp_links_set:
        final_links_list = sorted(list(st.session_state.found_whatsapp_links_set))
        df_links = pd.DataFrame(final_links_list, columns=["WhatsApp Group Link"])
        st.dataframe(df_links, use_container_width=True)

        csv_data = df_links.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="📥 Download All Found Links as CSV",
            data=csv_data,
            file_name=f"whatsapp_links_{get_domain(st.session_state.start_url_processed) or 'extracted'}.csv",
            mime='text/csv',
        )
    else:
        st.info("No WhatsApp group links were found.")

    if st.session_state.error_messages:
        with st.expander("⚠️ View All Encountered Errors/Warnings During Crawl"):
            st.json(st.session_state.error_messages)


st.markdown("---")
st.markdown("Built with [Streamlit](https://streamlit.io). "
            "**Warning:** Aggressive crawling can lead to IP blocks. Use responsibly and respect website terms.")
