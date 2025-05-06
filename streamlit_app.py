import streamlit as st
import requests
from bs4 import BeautifulSoup
import re
from urllib.parse import urljoin, urlparse
import pandas as pd
import time
from collections import deque

# --- Core Crawler Logic ---

# Regex to identify WhatsApp group links
WHATSAPP_LINK_PATTERN = re.compile(r'https://chat\.whatsapp\.com/([A-Za-z0-9]{20,24})') # Common invite code length

# Global set to store found WhatsApp links to avoid duplicates across different depths if needed
# For a single crawl session, a local set within crawl_website is sufficient.
# master_whatsapp_links = set() # Uncomment if you need a truly global store across multiple crawls (not typical for this app)

def is_valid_url(url):
    """Checks if a URL has a scheme and netloc."""
    parsed = urlparse(url)
    return bool(parsed.scheme) and bool(parsed.netloc)

def get_domain(url):
    """Extracts the domain (netloc) from a URL."""
    try:
        return urlparse(url).netloc
    except Exception:
        return None

def crawl_website(start_url, max_depth, polite_delay_seconds=0.5):
    """
    Crawls a website to extract WhatsApp group links.

    Args:
        start_url (str): The URL to begin crawling from.
        max_depth (int): The maximum depth to crawl.
        polite_delay_seconds (float): Delay between requests to be polite to servers.

    Returns:
        tuple: (set of found WhatsApp links, set of visited URLs)
    """
    if not is_valid_url(start_url):
        st.error(f"Invalid starting URL: {start_url}. Please include http/https.")
        return set(), set()

    found_whatsapp_links = set()
    visited_urls = set()
    # Queue stores tuples of (url, current_depth)
    queue = deque([(start_url, 0)])
    
    start_domain = get_domain(start_url)
    if not start_domain:
        st.error(f"Could not determine domain for start URL: {start_url}")
        return set(), set()

    # Initialize progress reporting
    crawled_count = 0
    status_text = st.empty() # Placeholder for status updates

    st.write(f"Starting crawl from: {start_url} (Max Depth: {max_depth})")
    st.write(f"Target domain: {start_domain}")

    while queue:
        current_url, current_depth = queue.popleft()

        if current_url in visited_urls or current_depth > max_depth:
            continue

        visited_urls.add(current_url)
        crawled_count += 1
        status_text.info(f"Crawling ({crawled_count}): {current_url} (Depth: {current_depth}) | Links in queue: {len(queue)}")

        try:
            # Add a User-Agent to look like a regular browser
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            response = requests.get(current_url, headers=headers, timeout=10) # 10-second timeout
            response.raise_for_status() # Raise HTTPError for bad responses (4XX or 5XX)
            
            soup = BeautifulSoup(response.content, 'html.parser') # Use response.content for better encoding handling

            # 1. Extract WhatsApp links from the current page
            for a_tag in soup.find_all('a', href=True):
                href = a_tag['href']
                # Try to form an absolute URL
                try:
                    full_url = urljoin(current_url, href)
                except ValueError:
                    # Handle cases where href might be malformed (e.g. "javascript:void(0)")
                    continue 
                
                if WHATSAPP_LINK_PATTERN.match(full_url) and full_url not in found_whatsapp_links:
                    found_whatsapp_links.add(full_url)
                    st.success(f"Found WhatsApp Link: {full_url}") # Immediate feedback

            # 2. Find new links on the page to crawl (if within depth and domain)
            if current_depth < max_depth:
                for a_tag in soup.find_all('a', href=True):
                    href = a_tag['href']
                    try:
                        next_url = urljoin(current_url, href)
                    except ValueError:
                        continue

                    # Clean URL (remove fragment)
                    next_url_parsed = urlparse(next_url)
                    next_url = next_url_parsed._replace(fragment="").geturl()


                    if (is_valid_url(next_url) and 
                        get_domain(next_url) == start_domain and 
                        next_url not in visited_urls and
                        # Ensure it's an HTTP/HTTPS link, not mailto: or tel: etc.
                        next_url_parsed.scheme in ['http', 'https']):
                        
                        # Add to queue only if not already processed or scheduled
                        # Check if (url, any_depth) is in queue - slightly complex for deque
                        # Simpler: if next_url is not in visited_urls, and we check visited_urls at queue.pop
                        # To avoid adding duplicates to the queue itself, can maintain a separate `queued_urls` set.
                        # For simplicity, we'll rely on the `visited_urls` check at the start of the loop.
                        # Many crawlers use a set for `to_visit` and `visited` for efficiency here.
                        queue.append((next_url, current_depth + 1))
            
            # Polite delay
            if polite_delay_seconds > 0:
                time.sleep(polite_delay_seconds)

        except requests.exceptions.RequestException as e:
            st.warning(f"Could not crawl {current_url}: {e}")
        except Exception as e:
            st.error(f"An unexpected error occurred while processing {current_url}: {e}")
            # Consider adding more specific error handling if needed

    status_text.info(f"Crawling finished. Visited {len(visited_urls)} pages.")
    return found_whatsapp_links, visited_urls

# --- Streamlit App UI ---
def main():
    st.set_page_config(page_title="WhatsApp Link Extractor", layout="wide")
    st.title("🔗 WhatsApp Group Link Extractor")
    st.markdown("""
    This app scans a website by crawling pages within the same domain to extract WhatsApp group join links.
    Be mindful of website terms of service and robots.txt before crawling.
    """)

    # Initialize session state for storing results if not already present
    if 'whatsapp_links' not in st.session_state:
        st.session_state.whatsapp_links = set()
    if 'visited_count' not in st.session_state:
        st.session_state.visited_count = 0
    if 'crawling_done' not in st.session_state:
        st.session_state.crawling_done = False


    with st.sidebar:
        st.header("⚙️ Crawler Settings")
        start_url_input = st.text_input("Enter Starting URL (e.g., https://example.com)", 
                                        value="https://blog.hubspot.com/marketing/whatsapp-groups", # Example with some links
                                        help="The website you want to scan.")
        max_depth_input = st.slider("Maximum Crawling Depth", 
                                    min_value=0, max_value=5, value=1, 
                                    help="0 for current page only, 1 for current page + direct links, etc. Deeper crawls take longer.")
        polite_delay = st.number_input("Polite Delay (seconds)", min_value=0.0, max_value=5.0, value=0.5, step=0.1,
                                       help="Delay between page requests to avoid overloading the server.")


    if st.sidebar.button("🚀 Start Crawling", type="primary"):
        if not start_url_input:
            st.sidebar.error("Please enter a starting URL.")
        else:
            st.session_state.whatsapp_links = set() # Reset previous results
            st.session_state.visited_count = 0
            st.session_state.crawling_done = False

            with st.spinner("🔍 Crawling in progress... Please wait."):
                # Placeholder for the actual crawl function
                # This will run synchronously, Streamlit's spinner handles it.
                extracted_links, visited = crawl_website(start_url_input, max_depth_input, polite_delay)
                st.session_state.whatsapp_links = extracted_links
                st.session_state.visited_count = len(visited)
                st.session_state.crawling_done = True
            
            st.balloons() # Fun little success indicator

    st.header("📊 Results")
    if st.session_state.crawling_done:
        st.info(f"Crawling complete. Visited {st.session_state.visited_count} pages.")
        if st.session_state.whatsapp_links:
            st.success(f"Found {len(st.session_state.whatsapp_links)} unique WhatsApp group links!")
            
            df_links = pd.DataFrame(list(st.session_state.whatsapp_links), columns=["WhatsApp Group Link"])
            st.dataframe(df_links, use_container_width=True)

            csv_data = df_links.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📥 Download Links as CSV",
                data=csv_data,
                file_name='whatsapp_group_links.csv',
                mime='text/csv',
            )
        else:
            st.warning("No WhatsApp group links found with the current settings.")
    else:
        st.info("Enter a URL and click 'Start Crawling' to see results.")

    st.markdown("---")
    st.markdown("Built with ❤️ by a Helpful Assistant & Streamlit")


if __name__ == "__main__":
    main()
