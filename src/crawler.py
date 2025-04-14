from mpi4py import MPI
import time
import logging
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from robotexclusionrulesparser import RobotExclusionRulesParser
import hashlib
import json
from datetime import datetime
import aiohttp
import asyncio
from collections import defaultdict
import os

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
        self.parser_cache = {}
        self.delay_cache = defaultdict(lambda: 1)  # Default delay of 1 second
        
    def get_robots_parser(self, domain):
        if domain not in self.parser_cache:
            parser = RobotExclusionRulesParser()
            try:
                robots_url = f"http://{domain}/robots.txt"
                parser.fetch(robots_url)
                self.parser_cache[domain] = parser
                # Extract crawl delay
                if parser.get_crawl_delay("*"):
                    self.delay_cache[domain] = parser.get_crawl_delay("*")
            except Exception as e:
                logging.warning(f"Could not fetch robots.txt for {domain}: {e}")
                self.parser_cache[domain] = None
        return self.parser_cache[domain]

    def can_fetch(self, url):
        domain = urlparse(url).netloc
        parser = self.get_robots_parser(domain)
        if parser is None:
            return True
        return parser.is_allowed("*", url)

    def get_delay(self, domain):
        return self.delay_cache[domain]

class WebCrawler:
    def __init__(self, rank):
        self.rank = rank
        self.session = None
        self.robots_cache = RobotsCache()
        self.last_crawl_time = defaultdict(float)
        self.headers = {
            'User-Agent': f'DistributedCrawler/1.0 (Educational Project; Rank {rank})'
        }
        # Load configuration
        try:
            with open('config/crawl_config.json', 'r') as f:
                self.config = json.load(f)
        except Exception as e:
            logging.warning(f"Could not load config, using defaults: {e}")
            self.config = {
                'max_depth': 3,
                'max_pages_per_domain': 1000,
                'respect_robots': True,
                'crawl_delay': 1.0,
                'allowed_domains': None
            }

    def should_crawl_domain(self, domain):
        """Check if domain is allowed based on configuration"""
        allowed_domains = self.config.get('allowed_domains')
        if allowed_domains:
            return domain in allowed_domains
        return True

    async def init_session(self):
        if self.session is None:
            self.session = aiohttp.ClientSession(headers=self.headers)

    async def close_session(self):
        if self.session:
            await self.session.close()
            self.session = None

    def respect_crawl_delay(self, domain):
        current_time = time.time()
        delay_needed = self.robots_cache.get_delay(domain)
        time_since_last_crawl = current_time - self.last_crawl_time[domain]
        
        if time_since_last_crawl < delay_needed:
            time.sleep(delay_needed - time_since_last_crawl)
        
        self.last_crawl_time[domain] = time.time()

    async def crawl_url(self, url, depth=0):
        try:
            domain = urlparse(url).netloc
            
            # Check depth limit
            if depth >= self.config.get('max_depth', 3):
                logging.info(f"Reached max depth {depth} for {url}")
                return [], None

            # Check domain restrictions
            if not self.should_crawl_domain(domain):
                logging.info(f"Domain {domain} not in allowed domains list")
                return [], None
            
            # Check robots.txt
            if not self.robots_cache.can_fetch(url):
                logging.info(f"URL {url} disallowed by robots.txt")
                return [], None
            
            # Respect crawl delay
            self.respect_crawl_delay(domain)
            
            await self.init_session()
            
            async with self.session.get(url, timeout=30) as response:
                if response.status != 200:
                    logging.warning(f"Got status code {response.status} for {url}")
                    return [], None
                
                content_type = response.headers.get('content-type', '').lower()
                if 'text/html' not in content_type:
                    logging.info(f"Skipping non-HTML content: {url}")
                    return [], None
                
                html = await response.text()
                
                # Save HTML content
                try:
                    url_hash = hashlib.md5(url.encode()).hexdigest()
                    os.makedirs('data/html', exist_ok=True)
                    html_file = os.path.join('data', 'html', f'{url_hash}.html')
                    with open(html_file, 'w', encoding='utf-8') as f:
                        f.write(html)
                except Exception as e:
                    logging.error(f"Error saving HTML for {url}: {e}")
                
                soup = BeautifulSoup(html, 'lxml')
                
                # Extract links
                links = []
                for link in soup.find_all('a', href=True):
                    href = link['href']
                    absolute_url = urljoin(url, href)
                    if absolute_url.startswith(('http://', 'https://')):
                        links.append(absolute_url)
                
                # Extract content for indexing
                # Remove script and style elements
                for script in soup(["script", "style"]):
                    script.decompose()
                
                text = soup.get_text(separator=' ', strip=True)
                title = soup.title.string if soup.title else ''
                
                content = {
                    'url': url,
                    'title': title,
                    'text': text,
                    'timestamp': datetime.utcnow().isoformat(),
                    'headers': dict(response.headers),
                }
                
                return links, content
                
        except Exception as e:
            logging.error(f"Error crawling {url}: {e}")
            return [], None

async def crawler_process():
    """
    Process for a crawler node.
    Fetches web pages, extracts URLs, and sends results back to the master.
    """
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    logging.info(f"Crawler node started with rank {rank} of {size}")
    
    crawler = WebCrawler(rank)
    
    try:
        while True:
            status = MPI.Status()
            message = comm.recv(source=0, tag=0, status=status)  # Receive URL and depth from master
            
            if not message:  # Shutdown signal
                logging.info(f"Crawler {rank} received shutdown signal. Exiting.")
                break
            
            if isinstance(message, dict):
                url = message['url']
                depth = message.get('depth', 0)
            else:
                url = message
                depth = 0
            
            logging.info(f"Crawler {rank} received URL: {url} at depth {depth}")
            
            try:
                extracted_urls, content = await crawler.crawl_url(url, depth)
                
                if content:
                    # Send extracted URLs back to master with depth information
                    url_messages = [{'url': u, 'depth': depth + 1} for u in extracted_urls]
                    comm.send(url_messages, dest=0, tag=1)
                    
                    # Send content to indexer nodes
                    crawler_nodes = size - 2
                    indexer_rank = 1 + crawler_nodes + (rank % 1)
                    comm.send(content, dest=indexer_rank, tag=2)
                    
                    # Send status update
                    status_msg = {
                        'url': url,
                        'urls_found': len(extracted_urls),
                        'success': True,
                        'timestamp': datetime.utcnow().isoformat(),
                        'depth': depth
                    }
                    comm.send(status_msg, dest=0, tag=99)
                else:
                    comm.send(f"Failed to crawl {url}", dest=0, tag=999)
            
            except Exception as e:
                logging.error(f"Crawler {rank} error crawling {url}: {e}")
                comm.send(f"Error crawling {url}: {e}", dest=0, tag=999)
    
    finally:
        await crawler.close_session()

if __name__ == '__main__':
    asyncio.run(crawler_process())