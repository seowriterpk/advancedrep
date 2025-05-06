import streamlit as st
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import pandas as pd
import re
import time
from collections import deque
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FutureTimeoutError
import queue # Thread-safe queue

# --- Constants ---
USER_AGENT = "WhatsAppLinkExtractor/2.1-debug (StreamlitApp; +https://your-github-repo)" # BE A GOOD BOT CITIZEN!
DEFAULT_REQUEST_DELAY = 0.5
CONNECT_TIMEOUT = 10
READ_TIMEOUT = 15

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
    return set(f"https://chat.whatsapp.com/{match}" for match in re.findall(pattern, page_content))

# --- Crawler Worker Function ---
def worker_task(url_to_crawl, current_depth, start_domain, visited_urls_lock, visited_urls_shared,
                results_queue_shared, new_links_to_process_queue,
                max_depth_crawl, stop_event, request_delay_worker):
    thread_id = threading.get_ident()
    print(f"[WORKER {thread_id}] STARTING for URL: {url_to_crawl} (Depth: {current_depth})")

    if stop_event.is_set():
        print(f"[WORKER {thread_id}] Stop event set. Exiting for {url_to_crawl}.")
        return None, [], 0

    # The URL given to this worker should already have been vetted (i.e., not in visited_urls_shared
    # when it was added to the main task queue). So, we proceed to process it.
    # The visited_urls_lock is for when THIS worker finds NEW links.

    pages_processed_by_worker = 0
    try:
        print(f"[WORKER {thread_id}] Sleeping for {request_delay_worker}s before request to {url_to_crawl}")
        time.sleep(request_delay_worker)
        headers = {'User-Agent': USER_AGENT}
        print(f"[WORKER {thread_id}] Making GET request to {url_to_crawl}")
        response = requests.get(url_to_crawl, headers=headers, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
        print(f"[WORKER {thread_id}] Response status {response.status_code} for {url_to_crawl}")
        response.raise_for_status()
        pages_processed_by_worker = 1 # Mark as processed (attempted and got response)

        content_type = response.headers.get('content-type', '').lower()
        if 'text/html' not in content_type:
            print(f"[WORKER {thread_id}] Skipping non-HTML content at {url_to_crawl} (type: {content_type})")
            return url_to_crawl, [], pages_processed_by_worker

        print(f"[WORKER {thread_id}] Parsing HTML for {url_to_crawl}")
        soup = BeautifulSoup(response.text, 'html.parser')

        page_whatsapp_links = find_whatsapp_links_on_page(response.text)
        if page_whatsapp_links:
            print(f"[WORKER {thread_id}] Found WhatsApp links on {url_to_crawl}: {page_whatsapp_links}")
            for link in page_whatsapp_links:
                results_queue_shared.put(link)
        else:
            print(f"[WORKER {thread_id}] No WhatsApp links found on {url_to_crawl}")


        found_internal_links_for_queue = []
        if current_depth < max_depth_crawl:
            print(f"[WORKER {thread_id}] Searching for internal links on {url_to_crawl} (current_depth: {current_depth}, max_depth: {max_depth_crawl})")
            for link_tag in soup.find_all('a', href=True):
                href = link_tag['href']
                absolute_link = urljoin(url_to_crawl, href)
                parsed_link = urlparse(absolute_link)
                clean_link = parsed_link._replace(fragment="", query="").geturl() # Remove fragment & query for queue

                if is_valid_url(clean_link) and get_domain(clean_link) == start_domain:
                    # This worker doesn't check visited_urls_shared here before adding to its *local* list.
                    # The main thread will do the check against visited_urls_shared before adding to the global task_queue.
                    found_internal_links_for_queue.append((clean_link, current_depth + 1))

            if found_internal_links_for_queue:
                print(f"[WORKER {thread_id}] Found {len(found_internal_links_for_queue)} potential internal links on {url_to_crawl}. Adding to new_links_to_process_queue.")
                new_links_to_process_queue.put(found_internal_links_for_queue)
            else:
                print(f"[WORKER {thread_id}] No new internal links found on {url_to_crawl} matching criteria.")
        else:
            print(f"[WORKER {thread_id}] Max depth reached for {url_to_crawl}. Not searching for more links.")

        print(f"[WORKER {thread_id}] FINISHED processing {url_to_crawl}")
        return url_to_crawl, list(page_whatsapp_links), pages_processed_by_worker

    except requests.exceptions.Timeout:
        print(f"[WORKER {thread_id}] Timeout fetching: {url_to_crawl}")
        st.session_state.error_messages.append(f"Timeout fetching: {url_to_crawl}")
        return url_to_crawl, [], pages_processed_by_worker # Count as attempted if pages_processed_by_worker was 0, else it's already 1
    except requests.exceptions.RequestException as e:
        print(f"[WORKER {thread_id}] Request Exception for {url_to_crawl}: {e}")
        st.session_state.error_messages.append(f"Failed: {url_to_crawl} ({type(e).__name__}: {e})")
        return url_to_crawl, [], pages_processed_by_worker
    except Exception as e:
        print(f"[WORKER {thread_id}] Generic Exception for {url_to_crawl}: {e}")
        st.session_state.error_messages.append(f"Error processing: {url_to_crawl} ({type(e).__name__}: {e})")
        return url_to_crawl, [], pages_processed_by_worker


# --- Streamlit App UI ---
st.set_page_config(page_title="Fast WA Link Extractor (Debug)", layout="wide")
st.title("🚀 Fast WhatsApp Group Link Extractor (Concurrent - Debug Mode)")
st.markdown("""
This app attempts to quickly scan a website using concurrent workers to extract WhatsApp group join links.
**Use responsibly! High concurrency can strain servers.**
Adjust concurrency, crawling depth, and view live progress.
**Check terminal/Streamlit Cloud logs for detailed [WORKER] messages.**
""")

# --- Initialize Session State ---
if 'crawling_in_progress' not in st.session_state: st.session_state.crawling_in_progress = False
if 'found_whatsapp_links_set' not in st.session_state: st.session_state.found_whatsapp_links_set = set()
if 'displayed_whatsapp_links' not in st.session_state: st.session_state.displayed_whatsapp_links = []
if 'pages_crawled_count' not in st.session_state: st.session_state.pages_crawled_count = 0
if 'urls_in_queue_count' not in st.session_state: st.session_state.urls_in_queue_count = 0
if 'start_url_processed' not in st.session_state: st.session_state.start_url_processed = ""
if 'error_messages' not in st.session_state: st.session_state.error_messages = []
if 'stop_event' not in st.session_state: st.session_state.stop_event = threading.Event()
if 'debug_log' not in st.session_state: st.session_state.debug_log = []


# --- Input Fields ---
with st.sidebar:
    st.header("⚙️ Crawler Settings")
    start_url_input = st.text_input("Enter Starting URL:", placeholder="e.g., https://example.com", value=st.session_state.get('last_start_url', "https://streamlit.io")) # Default for easier testing
    max_depth_input = st.slider("Crawling Depth:", 0, 5, 0) # Default to 0 for single page test
    num_workers_input = st.slider("Number of Concurrent Workers:", 1, 10, 1) # Default to 1 for easier debugging
    request_delay_input = st.number_input("Delay per Request (s):", 0.1, 5.0, DEFAULT_REQUEST_DELAY, 0.1)

col_start, col_stop = st.columns(2)
with col_start:
    if st.button("🚀 Start Crawling", type="primary", disabled=st.session_state.crawling_in_progress, use_container_width=True):
        st.session_state.debug_log = ["Start Crawling button clicked."] # Reset debug log
        if not start_url_input:
            st.error("⚠️ Please enter a starting URL.")
            st.session_state.debug_log.append("Error: No start URL.")
        elif not is_valid_url(start_url_input):
            st.error("⚠️ Please enter a valid URL (e.g., http:// or https://).")
            st.session_state.debug_log.append(f"Error: Invalid start URL: {start_url_input}")
        else:
            st.session_state.crawling_in_progress = True
            st.session_state.found_whatsapp_links_set = set()
            st.session_state.displayed_whatsapp_links = []
            st.session_state.pages_crawled_count = 0
            st.session_state.urls_in_queue_count = 0 # Will be updated shortly
            st.session_state.start_url_processed = start_url_input
            st.session_state.last_start_url = start_url_input
            st.session_state.error_messages = []
            st.session_state.stop_event.clear()
            st.session_state.debug_log.append(f"Crawl initiated for {start_url_input}, depth {max_depth_input}, workers {num_workers_input}.")

            st.session_state.task_queue = deque() # Initialize as empty, then add
            st.session_state.results_queue = queue.Queue()
            st.session_state.new_links_to_process_queue = queue.Queue()
            st.session_state.visited_urls = set()
            st.session_state.visited_urls_lock = threading.Lock()

            # Add initial URL
            with st.session_state.visited_urls_lock:
                parsed_initial = urlparse(start_url_input)
                initial_url_key = parsed_initial._replace(fragment="", query="").geturl()
                st.session_state.visited_urls.add(initial_url_key) # Mark as "identified for processing"
                st.session_state.task_queue.append((start_url_input, 0)) # Add original URL with fragment/query if any
                st.session_state.debug_log.append(f"Added initial URL {start_url_input} (key: {initial_url_key}) to task_queue and visited_urls.")

            st.session_state.urls_in_queue_count = len(st.session_state.task_queue)
            st.info(f"🚀 Crawl started for {start_url_input} with {num_workers_input} workers, depth {max_depth_input}.")
            st.rerun() # Rerun to enter the crawling loop

with col_stop:
    if st.button("🛑 Stop Crawling", disabled=not st.session_state.crawling_in_progress, use_container_width=True):
        st.session_state.debug_log.append("Stop Crawling button clicked.")
        st.session_state.stop_event.set()
        st.warning("🛑 Stop signal sent. Workers will finish current tasks and stop.")

# --- Live Status Placeholders ---
status_col1, status_col2, status_col3 = st.columns(3)
pages_crawled_placeholder = status_col1.empty()
links_found_placeholder = status_col2.empty()
queue_size_placeholder = status_col3.empty()
progress_bar_placeholder = st.empty()
live_links_header_placeholder = st.empty()
live_links_display_placeholder = st.empty()
error_messages_placeholder = st.empty()
debug_log_placeholder = st.empty()

# --- Main Crawling Loop (if active) ---
if st.session_state.crawling_in_progress:
    start_domain_crawl = get_domain(st.session_state.start_url_processed)
    st.session_state.debug_log.append(f"Main loop: Crawling in progress. Start domain: {start_domain_crawl}")

    if 'executor' not in st.session_state or st.session_state.executor is None:
        st.session_state.debug_log.append(f"Main loop: Creating ThreadPoolExecutor with {num_workers_input} workers.")
        st.session_state.executor = ThreadPoolExecutor(max_workers=num_workers_input, thread_name_prefix="CrawlerWorker")
        st.session_state.futures = []
    else:
        st.session_state.debug_log.append("Main loop: Executor already exists.")


    # Submit new tasks if workers are available and queue has items
    if st.session_state.executor and not st.session_state.stop_event.is_set():
        num_active_futures = len(st.session_state.futures)
        available_slots = num_workers_input - num_active_futures
        st.session_state.debug_log.append(f"Main loop: Active futures: {num_active_futures}, Available slots: {available_slots}, Task queue size: {len(st.session_state.task_queue)}")

        if available_slots > 0 and st.session_state.task_queue:
            tasks_to_submit_count = min(len(st.session_state.task_queue), available_slots)
            st.session_state.debug_log.append(f"Main loop: Will submit {tasks_to_submit_count} new tasks.")
            for _ in range(tasks_to_submit_count):
                if not st.session_state.task_queue: break # Should not happen if logic is correct
                url, depth = st.session_state.task_queue.popleft()
                st.session_state.debug_log.append(f"Main loop: Submitting task for {url} (depth {depth})")
                future = st.session_state.executor.submit(
                    worker_task, url, depth, start_domain_crawl,
                    st.session_state.visited_urls_lock,
                    st.session_state.visited_urls, # This is the shared set
                    st.session_state.results_queue,
                    st.session_state.new_links_to_process_queue,
                    max_depth_input,
                    st.session_state.stop_event,
                    request_delay_input
                )
                st.session_state.futures.append(future)
        elif available_slots <= 0:
             st.session_state.debug_log.append("Main loop: No available worker slots to submit new tasks.")
        elif not st.session_state.task_queue:
             st.session_state.debug_log.append("Main loop: Task queue is empty, nothing to submit.")


    # Process completed futures
    new_futures_list = []
    processed_futures_this_cycle = 0
    for future_idx, future in enumerate(st.session_state.futures):
        if future.done():
            processed_futures_this_cycle += 1
            st.session_state.debug_log.append(f"Main loop: Future {future_idx} is done.")
            if st.session_state.stop_event.is_set():
                st.session_state.debug_log.append(f"Main loop: Stop event set, not processing result of future {future_idx}.")
                continue

            try:
                # Add a timeout to future.result() to prevent indefinite blocking
                processed_url, _, pages_increment = future.result(timeout=5.0) # 5 sec timeout
                if pages_increment: # pages_increment is 0 or 1 from worker
                    st.session_state.pages_crawled_count += pages_increment
                st.session_state.debug_log.append(f"Main loop: Future {future_idx} result: url={processed_url}, pages_inc={pages_increment}")
            except FutureTimeoutError:
                st.session_state.error_messages.append(f"Worker task for future {future_idx} timed out on result retrieval.")
                st.session_state.debug_log.append(f"Main loop: Future {future_idx} timed out on result().")
                # Consider this an attempted page
                st.session_state.pages_crawled_count +=1
            except Exception as e:
                st.session_state.error_messages.append(f"Error getting result from worker: {e}")
                st.session_state.debug_log.append(f"Main loop: Future {future_idx} error on result(): {e}")
                # Consider this an attempted page
                st.session_state.pages_crawled_count +=1
        else:
            new_futures_list.append(future) # Keep non-done futures
    st.session_state.futures = new_futures_list
    if processed_futures_this_cycle > 0:
        st.session_state.debug_log.append(f"Main loop: Processed {processed_futures_this_cycle} completed futures this cycle.")


    # Process newly discovered internal links from workers
    new_links_added_to_task_queue = 0
    while not st.session_state.new_links_to_process_queue.empty():
        try:
            links_batch_from_worker = st.session_state.new_links_to_process_queue.get_nowait()
            st.session_state.debug_log.append(f"Main loop: Got batch of {len(links_batch_from_worker)} new links from a worker.")
            with st.session_state.visited_urls_lock:
                for link_url, link_depth in links_batch_from_worker:
                    parsed_new_link = urlparse(link_url)
                    new_link_key = parsed_new_link._replace(fragment="", query="").geturl() # Use cleaned key for visited check
                    if new_link_key not in st.session_state.visited_urls:
                        st.session_state.visited_urls.add(new_link_key)
                        st.session_state.task_queue.append((link_url, link_depth)) # Add original link to queue
                        new_links_added_to_task_queue += 1
                    else:
                        st.session_state.debug_log.append(f"Main loop: Link {new_link_key} already visited, not adding to task queue.")
        except queue.Empty: # Should not happen with while not empty, but good practice
            break
    if new_links_added_to_task_queue > 0:
        st.session_state.debug_log.append(f"Main loop: Added {new_links_added_to_task_queue} new unique links to task_queue.")


    # Collect WhatsApp links from results_queue
    new_wa_links_found_this_cycle = 0
    while not st.session_state.results_queue.empty():
        try:
            link = st.session_state.results_queue.get_nowait()
            if link not in st.session_state.found_whatsapp_links_set:
                st.session_state.found_whatsapp_links_set.add(link)
                st.session_state.displayed_whatsapp_links.append(link)
                new_wa_links_found_this_cycle += 1
        except queue.Empty:
            break
    if new_wa_links_found_this_cycle > 0:
        st.session_state.debug_log.append(f"Main loop: Collected {new_wa_links_found_this_cycle} new WhatsApp links.")


    # Update live counters
    st.session_state.urls_in_queue_count = len(st.session_state.task_queue) + len(st.session_state.futures)
    pages_crawled_placeholder.metric("Pages Crawled/Attempted", st.session_state.pages_crawled_count)
    links_found_placeholder.metric("Unique WhatsApp Links", len(st.session_state.found_whatsapp_links_set))
    queue_size_placeholder.metric("URLs in Queue (tasks + active)", st.session_state.urls_in_queue_count)

    total_urls_identified = len(st.session_state.visited_urls) # All URLs ever added to visited (queued or processed)
    if total_urls_identified > 0:
        progress_val = min(1.0, st.session_state.pages_crawled_count / total_urls_identified if total_urls_identified else 0)
        progress_bar_placeholder.progress(progress_val)
    else:
        progress_bar_placeholder.progress(0)

    if st.session_state.displayed_whatsapp_links:
        live_links_header_placeholder.subheader(f"🔗 Live View: Found WhatsApp Links ({len(st.session_state.displayed_whatsapp_links)})")
        links_text = "\n".join(sorted(st.session_state.displayed_whatsapp_links))
        live_links_display_placeholder.text_area("Links:", links_text, height=150, key="live_links_text_area_debug")

    if st.session_state.error_messages:
        error_messages_placeholder.expander("⚠️ Encountered Errors/Warnings").warning("\n".join(st.session_state.error_messages[-20:]))

    debug_log_placeholder.expander("🐞 Debug Log (Last 30 entries)").json(st.session_state.debug_log[-30:])


    # Check for crawl completion or stop signal
    if st.session_state.stop_event.is_set() or \
       (not st.session_state.task_queue and not st.session_state.futures): # No tasks left and no active workers
        st.session_state.debug_log.append("Main loop: Crawl completion condition met or stop event set.")
        st.session_state.crawling_in_progress = False
        if st.session_state.executor:
            st.session_state.debug_log.append("Main loop: Shutting down executor.")
            st.session_state.executor.shutdown(wait=True) # Wait for threads to finish their current task
            st.session_state.executor = None
            st.session_state.debug_log.append("Main loop: Executor shut down.")
        st.session_state.futures = []

        if st.session_state.stop_event.is_set():
            st.success("✅ Crawl stopped by user. Processed results up to this point are shown.")
        else:
            st.success(f"✅ Crawl finished! Attempted {st.session_state.pages_crawled_count} pages. Found {len(st.session_state.found_whatsapp_links_set)} unique WhatsApp links.")
        progress_bar_placeholder.progress(1.0)
        st.rerun() # Rerun one last time to update UI to non-crawling state

    # Rerun periodically to update UI from thread activity
    if st.session_state.crawling_in_progress:
        time.sleep(0.75) # Slightly longer to allow more debug logs to be read
        st.session_state.debug_log.append("Main loop: Rerunning script...")
        st.rerun()

# --- Display Final Results ---
if not st.session_state.crawling_in_progress and st.session_state.start_url_processed:
    st.markdown("---")
    st.subheader(f"📊 Final Results for {st.session_state.start_url_processed}")
    st.write(f"Total pages crawled/attempted: {st.session_state.pages_crawled_count}")
    st.write(f"Total unique WhatsApp links found: {len(st.session_state.found_whatsapp_links_set)}")

    if st.session_state.found_whatsapp_links_set:
        final_links_list = sorted(list(st.session_state.found_whatsapp_links_set))
        df_links = pd.DataFrame(final_links_list, columns=["WhatsApp Group Link"])
        st.dataframe(df_links, use_container_width=True)
        csv_data = df_links.to_csv(index=False).encode('utf-8')
        st.download_button("📥 Download Links as CSV", csv_data, f"whatsapp_links_{get_domain(st.session_state.start_url_processed) or 'extracted'}.csv", 'text/csv')
    else:
        st.info("No WhatsApp group links were found.")

    if st.session_state.error_messages:
        with st.expander("⚠️ View All Encountered Errors/Warnings During Crawl"):
            st.json(st.session_state.error_messages)
    if st.session_state.debug_log:
         debug_log_placeholder.expander("🐞 Final Debug Log").json(st.session_state.debug_log)


st.markdown("---")
st.caption("Built with Streamlit. Use responsibly and respect website terms.")
