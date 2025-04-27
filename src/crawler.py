import sys
import asyncio
import aiohttp
import time
import os
import hashlib
import json
import re
import logging
from urllib.parse import urljoin, urlparse
from collections import defaultdict
from datetime import datetime
import requests
from mpi4py import MPI

# Increase recursion limit carefully
sys.setrecursionlimit(3000)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - Crawler - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('crawler.log')
    ]
)

class RobotsCache:
    def __init__(self):
        self.allowed = defaultdict(lambda: True)
        self.delay = defaultdict(lambda: 1)

    def can_fetch(self, url):
        domain = urlparse(url).netloc
        return self.allowed[domain]

    def get_delay(self, domain):
        return self.delay[domain]

class WebCrawler:
    def __init__(self, rank):
        self.rank = rank
        self.session = None
        self.robots = RobotsCache()
        self.last_crawl_time = defaultdict(float)
        self.headers = {'User-Agent': f'CrawlBot/1.0 (Rank {rank})'}

        try:
            with open('config/crawl_config.json', 'r') as f:
                self.config = json.load(f)
        except Exception as e:
            logging.warning(f"Could not load config, using defaults: {e}")
            self.config = {
                'max_depth': 3,
                'max_pages_per_domain': 1000,
                'respect_robots': False,
                'crawl_delay': 1.0,
                'allowed_domains': None
            }

    async def init_session(self):
        if not self.session:
            self.session = aiohttp.ClientSession(headers=self.headers)

    async def close_session(self):
        if self.session:
            await self.session.close()

    def respect_delay(self, domain):
        now = time.time()
        needed = self.robots.get_delay(domain)
        waited = now - self.last_crawl_time[domain]
        if waited < needed:
            time.sleep(needed - waited)
        self.last_crawl_time[domain] = time.time()

    async def crawl_url(self, url, depth=0):
        if depth >= self.config.get('max_depth', 3):
            logging.info(f"Max depth reached for {url}")
            return [], None

        domain = urlparse(url).netloc
        allowed_domains = self.config.get('allowed_domains')
        if allowed_domains and allowed_domains[0] and domain not in allowed_domains:
            logging.info(f"Domain {domain} not allowed")
            return [], None

        if self.config.get('respect_robots') and not self.robots.can_fetch(url):
            logging.info(f"Blocked by robots.txt: {url}")
            return [], None

        self.respect_delay(domain)
        await self.init_session()

        try:
            async with self.session.get(url, timeout=20) as resp:
                if resp.status != 200:
                    logging.warning(f"HTTP {resp.status} for {url}")
                    return [], None
                
                ctype = resp.headers.get('content-type', '').lower()
                if 'text/html' not in ctype:
                    logging.info(f"Skipping non-HTML content: {url}")
                    return [], None

                html = await resp.text()
                links = self.extract_links(url, html)
                content = self.extract_content(url, html)
                return links, content
        except asyncio.TimeoutError:
            logging.error(f"Timeout {url}")
            return [], None
        except RecursionError:
            logging.error(f"Recursion error {url}")
            return [], None
        except Exception as e:
            logging.error(f"Error fetching {url}: {e}")
            return [], None

    def extract_links(self, base_url, html):
        try:
            links = []
            for match in re.finditer(r'href="(.*?)"', html):
                href = match.group(1)
                full_url = urljoin(base_url, href)
                if full_url.startswith('http'):
                    links.append(full_url)
            return links
        except Exception as e:
            logging.error(f"Link extraction error: {e}")
            return []

    def extract_content(self, url, html):
        try:
            title = ''
            title_match = re.search(r'<title>(.*?)</title>', html, re.IGNORECASE)
            if title_match:
                title = title_match.group(1)

            clean_html = re.sub(r'<script.*?</script>|<style.*?</style>', '', html, flags=re.DOTALL)
            text = re.sub(r'<[^>]+>', ' ', clean_html)
            text = ' '.join(text.split())

            url_hash = hashlib.md5(url.encode()).hexdigest()
            os.makedirs('data/html', exist_ok=True)
            with open(f'data/html/{url_hash}.html', 'w', encoding='utf-8') as f:
                f.write(html)

            return {
                'url': url,
                'title': title.strip(),
                'text': text.strip(),
                'timestamp': datetime.utcnow().isoformat()
            }
        except RecursionError:
            logging.error(f"Recursion error parsing {url}")
            return None
        except Exception as e:
            logging.error(f"Content extraction error {url}: {e}")
            return None

def cleanup_crawler():
    try:
        if os.path.exists('crawler.log'):
            os.remove('crawler.log')
        logging.info("Crawler cleanup completed")
    except Exception as e:
        logging.error(f"Error during cleanup: {e}")

def crawler_process():
    """Main process for crawler nodes"""
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    logging.info(f"Crawler node started with rank {rank} of {size}")

    crawler = WebCrawler(rank)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    while True:
        if comm.Iprobe(source=0, tag=MPI.ANY_TAG):
            message = comm.recv(source=0)

            if isinstance(message, dict) and message.get('command') == 'shutdown':
                logging.info("Received shutdown command from master")
                loop.run_until_complete(crawler.close_session())
                cleanup_crawler()
                break

            if isinstance(message, dict) and 'url' in message:
                url = message['url']
                depth = message.get('depth', 0)
                logging.info(f"Crawler {rank} crawling {url} at depth {depth}")

                links, content = loop.run_until_complete(crawler.crawl_url(url, depth))

                if content:
                    comm.send({'url': url, 'content': content['text'], 'meta': content}, dest=size-1, tag=2)
                    logging.info(f"Sent content for {url} to indexer")

                if links:
                    comm.send([{'url': link, 'depth': depth + 1} for link in links], dest=0, tag=1)
                    logging.info(f"Sent {len(links)} new URLs from {url} to master")

                comm.send({'url': url, 'status': 'completed'}, dest=0, tag=99)

def start_crawler():
    try:
        crawler_process()
    except KeyboardInterrupt:
        logging.info("Keyboard interrupt, shutting down crawler...")
        cleanup_crawler()
    except Exception as e:
        logging.error(f"Crawler crashed: {e}")
        cleanup_crawler()

if __name__ == '__main__':
    start_crawler()
