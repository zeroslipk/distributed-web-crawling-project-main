import os
import sys
import asyncio
import aiohttp
import time
import json
import re
import logging
import hashlib
from urllib.parse import urljoin, urlparse
from collections import defaultdict
from datetime import datetime
import uuid
import signal
import threading
from bs4 import BeautifulSoup
import random

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - GCP Crawler - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('gcpcrawler.log', mode='w')
    ]
)

# Late imports for Google Cloud services to allow local mode without GCP dependencies
google_cloud_available = True
try:
    from google.cloud import pubsub_v1, storage
except ImportError:
    google_cloud_available = False
    logging.warning("Google Cloud libraries not available. Will run in local-only mode.")

class RobotsCache:
    def __init__(self):
        self.allowed = defaultdict(lambda: True)
        self.delay = defaultdict(lambda: 1)

    def can_fetch(self, url):
        domain = urlparse(url).netloc
        return self.allowed[domain]

    def get_delay(self, domain):
        return self.delay[domain]

class GCPCrawler:
    def __init__(self, project_id=None, topic_name_prefix='web-crawler', local_mode=False):
        self.project_id = project_id
        self.worker_id = str(uuid.uuid4())[:8]  # Generate a unique worker ID
        self.local_mode = local_mode or not google_cloud_available
        
        # Initialize GCP clients if not in local mode
        self.gcp_available = False
        if not self.local_mode and google_cloud_available:
            try:
                self.publisher = pubsub_v1.PublisherClient()
                self.storage_client = storage.Client()
                self.gcp_available = True
                logging.info("GCP services initialized successfully")
            except Exception as e:
                logging.error(f"Failed to initialize GCP clients: {e}")
                self.local_mode = True
        
        # Create local directories for fallback
        os.makedirs('data/html', exist_ok=True)
        os.makedirs('data/urls/crawled', exist_ok=True)
        os.makedirs('data/urls/to_crawl', exist_ok=True)
        
        # Topic and subscription names
        if self.gcp_available:
            self.crawler_topic_name = f"projects/{project_id}/topics/{topic_name_prefix}-crawl-requests"
            self.results_topic_name = f"projects/{project_id}/topics/{topic_name_prefix}-results"
            self.heartbeat_topic_name = f"projects/{project_id}/topics/{topic_name_prefix}-heartbeats"
            
            # Create subscription for this crawler instance
            self.subscription_name = f"projects/{project_id}/subscriptions/{topic_name_prefix}-crawler-{self.worker_id}"
            self._create_subscription()
            
            # Cloud Storage bucket for HTML content
            self.bucket_name = f"{project_id}-crawler-data"
            self._ensure_bucket_exists()
        
        # Load config
        self.config = self._load_config()
        
        # HTTP session
        self.session = None
        
        # Setup robots cache
        self.robots = RobotsCache()
        self.last_crawl_time = defaultdict(float)
        self.headers = {'User-Agent': f'GCPCrawlBot/1.0 ({self.worker_id})'}
        
        # Track problematic domains to avoid repeatedly retrying them
        self.problematic_domains = set()
        
        # Subscriber client for pulling messages
        if self.gcp_available:
            self.subscriber = pubsub_v1.SubscriberClient()
        
        # Flag for controlling the crawler
        self.running = True
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Handle termination signals"""
        logging.info(f"Received signal {signum}, shutting down...")
        self.running = False

    def _create_subscription(self):
        """Create a Pub/Sub subscription for this crawler instance"""
        try:
            subscriber = pubsub_v1.SubscriberClient()
            try:
                subscriber.get_subscription(request={"subscription": self.subscription_name})
                logging.info(f"Subscription {self.subscription_name} already exists")
            except Exception:
                try:
                    subscription = subscriber.create_subscription(
                        request={
                            "name": self.subscription_name,
                            "topic": self.crawler_topic_name,
                            "ack_deadline_seconds": 60,
                            "message_retention_duration": {"seconds": 3600}  # 1 hour
                        }
                    )
                    logging.info(f"Created subscription: {subscription.name}")
                except Exception as e:
                    logging.error(f"Failed to create subscription: {e}")
                    logging.info("Will continue with limited functionality.")
            finally:
                subscriber.close()
        except Exception as e:
            logging.error(f"Error with subscription client: {e}")

    def _ensure_bucket_exists(self):
        """Make sure the Storage bucket exists"""
        try:
            self.storage_client.get_bucket(self.bucket_name)
            logging.info(f"Bucket {self.bucket_name} exists")
        except Exception:
            try:
                bucket = self.storage_client.create_bucket(self.bucket_name)
                logging.info(f"Created new bucket: {bucket.name}")
            except Exception as e:
                logging.error(f"Failed to create or access bucket: {e}")
                logging.info("Will use local storage for HTML content")

    def _load_config(self):
        """Load crawler configuration from cloud storage, local file, or environment variables"""
        config = {}
        
        # Try GCP storage if available
        if self.gcp_available:
            try:
                bucket = self.storage_client.bucket(self.bucket_name)
                blob = bucket.blob('config/crawler_config.json')
                
                if blob.exists():
                    config_str = blob.download_as_string()
                    config = json.loads(config_str)
                    logging.info("Loaded config from Cloud Storage")
                    return config
            except Exception as e:
                logging.error(f"Could not load config from Cloud Storage: {e}")
        
        # Try local file
        try:
            if os.path.exists('config/crawl_config.json'):
                with open('config/crawl_config.json', 'r') as f:
                    config = json.load(f)
                logging.info("Loaded config from local file")
                return config
        except Exception as e:
            logging.error(f"Could not load config from local file: {e}")
        
        # Try environment variables
        try:
            config['max_depth'] = int(os.environ.get('CRAWLER_MAX_DEPTH', 3))
            config['max_pages_per_domain'] = int(os.environ.get('CRAWLER_MAX_PAGES_PER_DOMAIN', 25))
            config['respect_robots'] = os.environ.get('CRAWLER_RESPECT_ROBOTS', 'true').lower() == 'true'
            config['crawl_delay'] = float(os.environ.get('CRAWLER_CRAWL_DELAY', 1.0))
            config['timeout'] = int(os.environ.get('CRAWLER_TIMEOUT', 60))
            config['max_retries'] = int(os.environ.get('CRAWLER_MAX_RETRIES', 5))
            
            allowed_domains = os.environ.get('CRAWLER_ALLOWED_DOMAINS', '')
            if allowed_domains:
                config['allowed_domains'] = [d.strip() for d in allowed_domains.split(',')]
            else:
                config['allowed_domains'] = []
            
            logging.info("Configured from environment variables")
            return config
        except Exception as e:
            logging.error(f"Error loading config from environment: {e}")
        
        # Default configuration
        logging.info("Using default configuration")
        return {
            'max_depth': 3,
            'max_pages_per_domain': 25,
            'respect_robots': True,
            'crawl_delay': 1.0,
            'allowed_domains': [],
            'timeout': 60,
            'max_retries': 5
        }

    async def init_session(self):
        """Initialize HTTP session"""
        if not self.session:
            # Create a connector that allows SSL certificate issues
            connector = aiohttp.TCPConnector(ssl=False)
            self.session = aiohttp.ClientSession(headers=self.headers, connector=connector)
            logging.info("Created HTTP session with SSL verification disabled for compatibility")

    async def close_session(self):
        """Close HTTP session"""
        if self.session:
            await self.session.close()
            self.session = None

    def respect_delay(self, domain):
        """Respect crawl delay for a domain"""
        now = time.time()
        needed = self.robots.get_delay(domain)
        waited = now - self.last_crawl_time[domain]
        if waited < needed:
            time.sleep(needed - waited)
        self.last_crawl_time[domain] = time.time()

    async def crawl_url(self, url, depth=0):
        """Crawl a given URL and extract content and links"""
        domain = urlparse(url).netloc
        
        # Check if this is a known problematic domain
        if domain in self.problematic_domains:
            logging.warning(f"Skipping known problematic domain: {domain}")
            return {
                'url': url,
                'depth': depth,
                'success': False,
                'error': 'Known problematic domain',
                'links': [],
                'content': None,
                'timestamp': datetime.now().isoformat()
            }
        
        # Respect crawl delay for robots.txt
        self.respect_delay(domain)
        
        try:
            # Log crawl attempt
            logging.info(f"Crawling URL: {url}")
            
            # Get timeout from config
            timeout = self.config.get('timeout', 60)
            max_retries = self.config.get('max_retries', 5)

            # Use aiohttp for HTTP requests
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Cache-Control': 'max-age=0',
            }
            
            # Enhanced exponential backoff retry logic
            retry_delay = 2
            
            for retry in range(max_retries):
                try:
                    async with self.session.get(url, headers=headers, timeout=timeout, allow_redirects=True) as response:
                        # Check if the response was successful
                        if response.status == 200:
                            # Get content type
                            content_type = response.headers.get('Content-Type', '').lower()
                            
                            # Only process HTML content
                            if 'text/html' in content_type:
                                # Get HTML content
                                html_content = await response.text()
                                
                                # Parse HTML
                                soup = BeautifulSoup(html_content, 'html.parser')
                                
                                # Extract links
                                links = []
                                for link in soup.find_all('a', href=True):
                                    href = link['href'].strip()
                                    if not href:
                                        continue
                                        
                                    # Skip javascript links and anchors
                                    if href.startswith(('javascript:', '#', 'mailto:', 'tel:')):
                                        continue
                                        
                                    # Convert relative URLs to absolute
                                    absolute_url = urljoin(url, href)
                                    
                                    # Remove fragments
                                    absolute_url = absolute_url.split('#')[0]
                                    
                                    # Ensure URL has proper format
                                    if absolute_url.startswith(('http://', 'https://')):
                                        links.append(absolute_url)
                                
                                # Extract text content
                                text_content = soup.get_text(separator=' ', strip=True)
                                
                                # Extract title
                                title = soup.title.string if soup.title else url
                                
                                # Return successful result
                                return {
                                    'url': url,
                                    'title': title,
                                    'links': links,
                                    'content': text_content,
                                    'html': html_content,
                                    'depth': depth,
                                    'success': True,
                                    'timestamp': datetime.now().isoformat()
                                }
                            else:
                                # Non-HTML content
                                logging.info(f"Skipping non-HTML content for {url}: {content_type}")
                                return {
                                    'url': url,
                                    'depth': depth,
                                    'success': True,  # Mark as success to avoid re-crawling
                                    'content_type': content_type,
                                    'links': [],
                                    'content': None,
                                    'timestamp': datetime.now().isoformat()
                                }
                        elif response.status in [301, 302, 303, 307, 308]:
                            # Handle redirects manually if needed
                            redirect_url = response.headers.get('Location')
                            if redirect_url:
                                logging.info(f"Redirect from {url} to {redirect_url}")
                                # Follow the redirect (up to a limit)
                                if retry < 3:  # Allow a few redirects
                                    url = urljoin(url, redirect_url)
                                    continue
                                else:
                                    return {
                                        'url': url,
                                        'depth': depth,
                                        'success': False,
                                        'error': f'Too many redirects',
                                        'links': [],
                                        'content': None,
                                        'timestamp': datetime.now().isoformat()
                                    }
                        elif response.status == 429:
                            # Too Many Requests - retry with exponential backoff
                            wait_time = retry_delay * (2 ** retry) + random.uniform(1, 5)  # Add jitter
                            logging.warning(f"Rate limited (429) for {url}, retrying in {wait_time} seconds (attempt {retry+1}/{max_retries})")
                            await asyncio.sleep(wait_time)
                            continue
                        elif response.status >= 500:
                            # Server error - retry with exponential backoff
                            if retry < max_retries - 1:
                                wait_time = retry_delay * (2 ** retry)
                                logging.warning(f"Server error {response.status} for {url}, retrying in {wait_time} seconds (attempt {retry+1}/{max_retries})")
                                await asyncio.sleep(wait_time)
                                continue
                            else:
                                logging.warning(f"Server error {response.status} for {url} after {max_retries} attempts")
                                return {
                                    'url': url,
                                    'depth': depth,
                                    'success': False,
                                    'error': f'HTTP status {response.status} (server error)',
                                    'http_status': response.status,
                                    'links': [],
                                    'content': None,
                                    'timestamp': datetime.now().isoformat()
                                }
                        else:
                            # Other HTTP error
                            logging.warning(f"HTTP error for {url}: {response.status}")
                            return {
                                'url': url,
                                'depth': depth,
                                'success': False,
                                'error': f'HTTP status {response.status}',
                                'http_status': response.status,
                                'links': [],
                                'content': None,
                                'timestamp': datetime.now().isoformat()
                            }
                    # If we got here, the request was successful
                    break
                except asyncio.TimeoutError:
                    if retry < max_retries - 1:
                        # Try again with exponential backoff
                        wait_time = retry_delay * (2 ** retry)
                        logging.warning(f"Timeout for {url}, retrying in {wait_time} seconds (attempt {retry+1}/{max_retries})")
                        await asyncio.sleep(wait_time)
                    else:
                        # Final timeout, report failure
                        logging.warning(f"Timeout while crawling URL: {url} after {max_retries} attempts")
                        return {
                            'url': url,
                            'depth': depth,
                            'success': False,
                            'error': 'Request timeout',
                            'error_detail': f'Timeout after {max_retries} attempts with {timeout}s timeout each',
                            'links': [],
                            'content': None,
                            'timestamp': datetime.now().isoformat()
                        }
                except aiohttp.ClientConnectorError as e:
                    # Connection error (DNS failure, refused connection, etc)
                    if retry < max_retries - 1:
                        wait_time = retry_delay * (2 ** retry)
                        logging.warning(f"Connection error for {url}, retrying in {wait_time} seconds (attempt {retry+1}/{max_retries}): {e}")
                        await asyncio.sleep(wait_time)
                    else:
                        logging.warning(f"Connection error for {url} after {max_retries} attempts: {e}")
                        # Add domain to problematic domains list after multiple connection failures
                        self.problematic_domains.add(domain)
                        logging.warning(f"Added {domain} to problematic domains list")
                        return {
                            'url': url,
                            'depth': depth,
                            'success': False,
                            'error': f'Connection error: {str(e)}',
                            'links': [],
                            'content': None,
                            'timestamp': datetime.now().isoformat()
                        }
                except aiohttp.ClientSSLError as e:
                    # SSL certificate errors
                    if retry < max_retries - 1:
                        wait_time = retry_delay * (2 ** retry)
                        logging.warning(f"SSL error for {url}, retrying in {wait_time} seconds (attempt {retry+1}/{max_retries}): {e}")
                        await asyncio.sleep(wait_time)
                    else:
                        logging.warning(f"SSL error for {url} after {max_retries} attempts: {e}")
                        # Add domain to problematic domains list
                        self.problematic_domains.add(domain)
                        logging.warning(f"Added {domain} to problematic domains list due to SSL errors")
                        return {
                            'url': url,
                            'depth': depth,
                            'success': False,
                            'error': f'SSL error: {str(e)}',
                            'links': [],
                            'content': None,
                            'timestamp': datetime.now().isoformat()
                        }
                except aiohttp.ClientError as e:
                    # Other client errors
                    if retry < max_retries - 1:
                        wait_time = retry_delay * (2 ** retry)
                        logging.warning(f"Client error for {url}, retrying in {wait_time} seconds (attempt {retry+1}/{max_retries}): {e}")
                        await asyncio.sleep(wait_time)
                    else:
                        logging.warning(f"Client error for {url} after {max_retries} attempts: {e}")
                        # Add domain to problematic domains list after multiple client errors
                        self.problematic_domains.add(domain)
                        logging.warning(f"Added {domain} to problematic domains list")
                        return {
                            'url': url,
                            'depth': depth,
                            'success': False,
                            'error': f'Client error: {str(e)}',
                            'links': [],
                            'content': None,
                            'timestamp': datetime.now().isoformat()
                        }
                except Exception as e:
                    # Other unexpected errors - attempt retry for some common issues
                    if retry < max_retries - 1 and any(err in str(e).lower() for err in ['timeout', 'reset', 'refused', 'connection', 'temporary']):
                        wait_time = retry_delay * (2 ** retry)
                        logging.warning(f"Error during retry {retry+1} for {url}, will retry in {wait_time} seconds: {str(e)}")
                        await asyncio.sleep(wait_time)
                    else:
                        # Break the retry loop on other errors
                        logging.error(f"Error during retry {retry+1} for {url}: {str(e)}")
                        break
            
            # All retries failed with an unknown error
            return {
                'url': url,
                'depth': depth,
                'success': False,
                'error': 'Failed after multiple retries',
                'links': [],
                'content': None,
                'timestamp': datetime.now().isoformat()
            }
        except Exception as e:
            # Catch any unexpected exceptions outside the retry loop
            logging.error(f"Unexpected error crawling URL {url}: {str(e)}")
            return {
                'url': url,
                'depth': depth,
                'success': False,
                'error': str(e),
                'links': [],
                'content': None,
                'timestamp': datetime.now().isoformat()
            }

    def extract_links(self, base_url, html):
        """Extract links from HTML"""
        try:
            links = []
            # Simple regex for href extraction
            for match in re.finditer(r'href=[\'"]([^\'"]+)[\'"]', html):
                href = match.group(1)
                full_url = urljoin(base_url, href)
                if full_url.startswith('http'):
                    links.append(full_url)
            return links
        except Exception as e:
            logging.error(f"Link extraction error: {e}")
            return []

    def extract_content(self, url, html):
        """Extract content from HTML"""
        try:
            title = ''
            title_match = re.search(r'<title>(.*?)</title>', html, re.IGNORECASE | re.DOTALL)
            if title_match:
                title = title_match.group(1)

            # Simple HTML cleaning
            clean_html = re.sub(r'<script.*?</script>|<style.*?</style>', '', html, flags=re.DOTALL)
            text = re.sub(r'<[^>]+>', ' ', clean_html)
            text = ' '.join(text.split())

            return {
                'url': url,
                'title': title.strip(),
                'text': text.strip(),
                'timestamp': datetime.utcnow().isoformat()
            }
        except Exception as e:
            logging.error(f"Content extraction error {url}: {e}")
            return None

    def save_html_content(self, url, html):
        """Save HTML content to storage or local file"""
        safe_url = self._safe_filename(url)
        
        if self.gcp_available:
            try:
                # Save to Cloud Storage
                bucket = self.storage_client.bucket(self.bucket_name)
                blob = bucket.blob(f"html/{safe_url}.html")
                blob.upload_from_string(html, content_type='text/html')
                logging.debug(f"Saved HTML content to Cloud Storage: {url}")
                return True
            except Exception as e:
                logging.error(f"Failed to save HTML content to Cloud Storage: {e}")
                # Fall back to local storage
        
        # Save to local storage
        try:
            os.makedirs('data/html', exist_ok=True)
            with open(f"data/html/{safe_url}.html", 'w', encoding='utf-8') as f:
                f.write(html)
            logging.debug(f"Saved HTML content locally: {url}")
            return True
        except Exception as e:
            logging.error(f"Failed to save HTML content locally: {e}")
            return False
    
    def _safe_filename(self, url):
        """Convert URL to a safe filename"""
        # Remove protocol and replace special characters
        clean_url = url.replace('://', '_').replace('/', '_').replace('?', '_').replace('&', '_')
        # If the filename is too long, hash the end
        if len(clean_url) > 180:
            hash_suffix = hashlib.md5(url.encode()).hexdigest()[:16]
            clean_url = clean_url[:160] + '_' + hash_suffix
        return clean_url

    async def process_message(self, message):
        """Process a crawl request message"""
        try:
            if isinstance(message, str):
                # Message from local file
                data = json.loads(message)
                pubsub_message = False
            elif hasattr(message, 'data'):
                # Message directly from Pub/Sub client
                data = json.loads(message.data.decode('utf-8'))
                pubsub_message = True
            else:
                # Message from Pub/Sub pull response
                data = json.loads(message.message.data.decode('utf-8'))
                pubsub_message = True
            
            url = data.get('url')
            depth = data.get('depth', 0)
            
            if not url:
                logging.warning("Received message without URL")
                return
            
            # Fix URLs missing protocol
            if not url.startswith(('http://', 'https://')):
                original_url = url
                url = 'https://' + url
                logging.info(f"Added https:// protocol to URL: {original_url} -> {url}")
            
            # Crawl the URL
            result = await self.crawl_url(url, depth)
            
            # Ensure result has all required fields
            if result is None:
                result = {
                    'url': url,
                    'depth': depth,
                    'success': False,
                    'error': 'Unknown error',
                    'links': [],
                    'content': None,
                    'timestamp': datetime.now().isoformat()
                }
            
            # Ensure result has links even if null
            if 'links' not in result:
                result['links'] = []
            
            # Ensure result has content even if null
            if 'content' not in result:
                result['content'] = None
            
            # Send result back to master
            self.send_result(
                url, 
                result.get('links', []), 
                result.get('content'), 
                result.get('success', False), 
                depth, 
                result.get('error'), 
                result.get('error_detail')
            )
            
            logging.info(f"Processed URL: {url}, success: {result.get('success', False)}, links: {len(result.get('links', [])) if result.get('links') else 0}")
            
        except Exception as e:
            logging.error(f"Error processing message: {e}")
            # Try to send a failure result if we have a URL
            if 'url' in locals():
                try:
                    self.send_result(
                        url, 
                        [], 
                        None, 
                        False, 
                        depth if 'depth' in locals() else 0, 
                        "Error processing message", 
                        str(e)
                    )
                    logging.info(f"Sent failure result for {url}")
                except Exception as e2:
                    logging.error(f"Error sending failure result: {e2}")

    def send_result(self, url, links, content, success=True, depth=0, error=None, error_detail=None):
        """Send crawl result back to the master"""
        # Ensure links is a list
        if links is None:
            links = []
        
        result = {
            'url': url,
            'links': links,
            'success': success,
            'timestamp': datetime.now().isoformat(),
            'depth': depth
        }
        
        # Add error information if provided
        if error:
            result['error'] = error
        
        if error_detail:
            result['error_detail'] = error_detail
        
        # Only include content if it was extracted successfully
        if content:
            result['content'] = content
        
        if self.gcp_available:
            try:
                # Send to Pub/Sub - use only data with no additional attributes
                message_json = json.dumps(result).encode('utf-8')
                future = self.publisher.publish(
                    self.results_topic_name, 
                    data=message_json
                )
                message_id = future.result()
                logging.debug(f"Sent result to Pub/Sub: {url} (ID: {message_id})")
                return True
            except Exception as e:
                logging.error(f"Failed to send result to Pub/Sub: {e}")
                # Fall back to local storage
        
        # If Pub/Sub unavailable or failed, save result to file
        try:
            result_file = self._save_result_to_storage(url, result)
            logging.debug(f"Saved result to file: {url} -> {result_file}")
            return True
        except Exception as e:
            logging.error(f"Failed to save result: {e}")
            return False

    def _save_result_to_storage(self, url, result):
        """Save crawl result to local storage"""
        try:
            os.makedirs('data/urls/crawled', exist_ok=True)
            safe_filename = self._safe_filename(url)
            filename = f'data/urls/crawled/{safe_filename}.json'
            with open(filename, 'w') as f:
                json.dump(result, f)
            return filename
        except Exception as e:
            logging.error(f"Error saving result to storage: {e}")
            # Try with a more generic filename
            try:
                filename = f'data/urls/crawled/{uuid.uuid4()}.json'
                with open(filename, 'w') as f:
                    json.dump(result, f)
                return filename
            except Exception as e2:
                logging.error(f"Error saving result with alternative filename: {e2}")
                raise

    def send_heartbeat(self):
        """Send heartbeat to signal this crawler is alive"""
        if not self.gcp_available:
            return False
            
        try:
            message_data = {
                'worker_id': self.worker_id,
                'component': 'crawler',
                'timestamp': datetime.now().isoformat(),
                'status': 'running'
            }
            
            # Publish message with data only, no additional attributes
            future = self.publisher.publish(
                self.heartbeat_topic_name, 
                data=json.dumps(message_data).encode('utf-8')
            )
            future.result()
            logging.debug("Sent heartbeat")
            return True
        except Exception as e:
            logging.error(f"Failed to send heartbeat: {e}")
            return False

    def check_local_tasks(self):
        """Check for crawl tasks in the local file system"""
        # First check if there's a flag to clear problematic domains
        root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        reset_flag_path = os.path.join(root_dir, 'clear_problematic_domains.flag')
        
        if os.path.exists(reset_flag_path):
            try:
                # Read the flag file to log the reset request
                with open(reset_flag_path, 'r') as f:
                    reset_info = f.read().strip()
                
                # Clear the problematic domains set
                domain_count = len(self.problematic_domains)
                self.problematic_domains.clear()
                logging.info(f"Cleared {domain_count} problematic domains from cache. Reset request: {reset_info}")
                
                # Delete the flag file
                os.remove(reset_flag_path)
            except Exception as e:
                logging.error(f"Error processing problematic domains reset flag: {e}")
        
        # Continue with normal task checking
        task_dir = 'data/urls/to_crawl'
        if not os.path.exists(task_dir):
            return None
            
        files = [f for f in os.listdir(task_dir) if f.endswith('.json')]
        if not files:
            return None
            
        # Process one file
        filepath = os.path.join(task_dir, files[0])
        try:
            with open(filepath, 'r') as f:
                message = f.read()
            # Remove the file after reading
            os.remove(filepath)
            return message
        except Exception as e:
            logging.error(f"Error processing local task file {filepath}: {e}")
            # Try to move the file to a failed directory
            try:
                os.makedirs('data/urls/failed', exist_ok=True)
                os.rename(filepath, f"data/urls/failed/{os.path.basename(filepath)}")
            except:
                pass
            return None

    def crawl_from_file(self, url_file):
        """Crawl URLs from a file"""
        urls = []
        try:
            with open(url_file, 'r') as f:
                for line in f:
                    url = line.strip()
                    if url and not url.startswith('#'):
                        urls.append(url)
            
            logging.info(f"Loaded {len(urls)} URLs from file: {url_file}")
            
            # Process URLs with asyncio
            async def process_urls():
                for url in urls:
                    result = await self.crawl_url(url)
                    self.send_result(url, result['links'], result['content'], result['success'], result['depth'], result.get('error'), result.get('error_detail'))
            
            # Run the async function
            loop = asyncio.get_event_loop()
            loop.run_until_complete(process_urls())
            
            return True
        except Exception as e:
            logging.error(f"Error crawling from file: {e}")
            return False

    async def process_local_message(self, message_str):
        """Process a message from local storage"""
        await self.process_message(message_str)

    async def run_async(self):
        """Run the crawler in async mode - works for both cloud and local"""
        await self.init_session()
        
        try:
            while self.running:
                # If in GCP mode, pull from Pub/Sub
                if self.gcp_available:
                    try:
                        response = self.subscriber.pull(
                            request={
                                "subscription": self.subscription_name,
                                "max_messages": 5,
                            }
                        )
                        
                        if response.received_messages:
                            for received_message in response.received_messages:
                                await self.process_message(received_message)
                                
                                # Acknowledge the message after processing
                                self.subscriber.acknowledge(
                                    request={
                                        "subscription": self.subscription_name,
                                        "ack_ids": [received_message.ack_id],
                                    }
                                )
                    except Exception as e:
                        logging.error(f"Error pulling messages: {e}")
                
                # Always check local files too (fallback or local-only mode)
                message = self.check_local_tasks()
                if message:
                    await self.process_local_message(message)
                
                # No messages to process, sleep before checking again
                await asyncio.sleep(1)
        
        except KeyboardInterrupt:
            logging.info("Keyboard interrupt received, shutting down")
        except Exception as e:
            logging.error(f"Error in crawler run loop: {e}")
        finally:
            # Clean up
            await self.close_session()
            if self.gcp_available:
                try:
                    self.subscriber.close()
                except:
                    pass

    def run(self):
        """Run the crawler"""
        # Start heartbeat thread if using GCP
        if self.gcp_available:
            heartbeat_thread = threading.Thread(target=self._heartbeat_loop)
            heartbeat_thread.daemon = True
            heartbeat_thread.start()
        
        # Run the async loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            loop.run_until_complete(self.run_async())
        finally:
            loop.close()

    def _heartbeat_loop(self):
        """Send periodic heartbeats"""
        while self.running:
            self.send_heartbeat()
            time.sleep(30)  # Send heartbeat every 30 seconds

def main(local_mode=False):
    """Main function to start the crawler"""
    # Get project ID from environment or service account
    project_id = os.environ.get('GOOGLE_CLOUD_PROJECT')
    if not project_id and not local_mode:
        try:
            # Try to get from service account file
            cred_file = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
            if cred_file and os.path.exists(cred_file):
                with open(cred_file, 'r') as f:
                    creds = json.load(f)
                    project_id = creds.get('project_id')
        except Exception as e:
            logging.error(f"Error getting project ID: {e}")
    
    if not project_id and not local_mode:
        logging.warning("No project ID found. Using 'local-project' as fallback.")
        project_id = 'local-project'
        
    # Create and run the crawler
    crawler = GCPCrawler(project_id=project_id, local_mode=local_mode)
    crawler.run()

if __name__ == '__main__':
    main() 