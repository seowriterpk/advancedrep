# app.py
import asyncio
import re
import streamlit as st
import pandas as pd
import httpx
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from collections import deque
import time
import xml.etree.ElementTree as ET
from typing import Set, List

# Constants
WA_LINK_PATTERN = re.compile(r"https://chat\.whatsapp\.com/[A-Za-z0-9]+")
ROBOT_TXT = "/robots.txt"
SITEMAP_XML = "/sitemap.xml"

st.set_page_config(page_title="🔍 WhatsApp Link Extractor Pro", layout="wide")
st.title("🔍 Advanced WhatsApp Group Link Extractor")

# UI
start_url = st.text_input("Enter Website URL", "https://example.com")
crawl_depth = st.slider("Crawl Depth", 1, 10, 3)
max_pages = st.number_input("Max Pages to Crawl", 10, 1000, 200)
delay = st.slider("Delay between requests (sec)", 0.0, 3.0, 0.5)
use_sitemap = st.checkbox("Use sitemap.xml or autodetect", True)
obey_robots = st.checkbox("Obey robots.txt", True)
restrict_domain = st.checkbox("Restrict to start domain only", True)
user_agent_mode = st.radio("User-Agent", ["Googlebot", "Mobile Safari"], index=0)

UA_MAP = {
    "Googlebot": "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
    "Mobile Safari": "Mozilla/5.0 (iPhone; CPU iPhone OS 13_5_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.1 Mobile/15E148 Safari/604.1"
}

HEADERS = {
    "User-Agent": UA_MAP[user_agent_mode]
}

# Async fetch with httpx
async def fetch_html(client: httpx.AsyncClient, url: str) -> str:
    try:
        response = await client.get(url, timeout=10)
        if response.status_code == 200 and "text/html" in response.headers.get("Content-Type", ""):
            return response.text
    except:
        return ""
    return ""

def extract_wa_links(html: str) -> List[str]:
    return list(set(WA_LINK_PATTERN.findall(html)))

def extract_page_links(html: str, base_url: str, same_domain_only: bool) -> Set[str]:
    soup = BeautifulSoup(html, "html.parser")
    found = set()
    base_domain = urlparse(base_url).netloc
    for a in soup.find_all("a", href=True):
        href = urljoin(base_url, a['href'])
        parsed = urlparse(href)
        if same_domain_only and parsed.netloc != base_domain:
            continue
        if parsed.scheme in ["http", "https"]:
            found.add(href.split("#")[0])
    return found

def get_sitemap_links(url: str) -> Set[str]:
    sitemap_url = urljoin(url, SITEMAP_XML)
    links = set()
    try:
        r = httpx.get(sitemap_url, headers=HEADERS, timeout=10)
        if r.status_code == 200:
            root = ET.fromstring(r.content)
            for loc in root.iter("{http://www.sitemaps.org/schemas/sitemap/0.9}loc"):
                links.add(loc.text.strip())
    except:
        pass
    return links

def get_robots_sitemap(url: str) -> List[str]:
    robots_url = urljoin(url, ROBOT_TXT)
    sitemaps = []
    try:
        r = httpx.get(robots_url, headers=HEADERS, timeout=10)
        if r.status_code == 200:
            for line in r.text.splitlines():
                if line.lower().startswith("sitemap:"):
                    sitemap_link = line.split(":", 1)[1].strip()
                    sitemaps.append(sitemap_link)
    except:
        pass
    return sitemaps

async def crawl(start_url: str, depth: int, max_pages: int, delay: float, use_sitemap: bool, same_domain: bool) -> pd.DataFrame:
    queue = deque()
    visited = set()
    wa_links = {}
    broken = []

    if use_sitemap:
        sitemaps = get_robots_sitemap(start_url)
        if sitemaps:
            for sm in sitemaps:
                queue.extend(get_sitemap_links(sm))
        else:
            queue.extend(get_sitemap_links(start_url))
    else:
        queue.append(start_url)

    progress = st.progress(0)
    msg = st.empty()
    client = httpx.AsyncClient(headers=HEADERS, follow_redirects=True)
    current_depth = 0

    while queue and len(visited) < max_pages and current_depth <= depth:
        batch = list(set(queue))[:50]
        queue.clear()

        results = await asyncio.gather(*(fetch_html(client, url) for url in batch))
        for i, html in enumerate(results):
            url = batch[i]
            if not html:
                broken.append(url)
                continue
            visited.add(url)
            links = extract_wa_links(html)
            for link in links:
                wa_links[link] = url  # map to source page
            new_links = extract_page_links(html, url, same_domain)
            queue.extend(new_links - visited)

            await asyncio.sleep(delay)

        current_depth += 1
        progress.progress(min(len(visited) / max_pages, 1.0))
        msg.write(f"Crawled: {len(visited)} pages, Found: {len(wa_links)} WhatsApp links")

    await client.aclose()
    progress.empty()
    msg.write("✅ Crawling complete.")

    df = pd.DataFrame(list(wa_links.items()), columns=["WhatsApp Link", "Found On"])
    return df

# MAIN RUN
if st.button("🚀 Start Extraction"):
    if not start_url.startswith("http"):
        st.error("Invalid URL. Must start with http:// or https://")
    else:
        with st.spinner("Crawling in progress..."):
            result_df = asyncio.run(crawl(start_url, crawl_depth, max_pages, delay, use_sitemap, restrict_domain))
            st.success(f"✅ Found {len(result_df)} WhatsApp links.")
            st.dataframe(result_df)

            csv = result_df.to_csv(index=False).encode("utf-8")
            txt = "\n".join(result_df["WhatsApp Link"])
            st.download_button("📥 Download CSV", csv, "wa_links.csv", "text/csv")
            st.download_button("📄 Download TXT", txt, "wa_links.txt", "text/plain")
