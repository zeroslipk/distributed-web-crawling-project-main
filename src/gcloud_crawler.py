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
            'allowed_domains': []
        }

    async def init_session(self):
        """Initialize HTTP session"""
        if not self.session:
            self.session = aiohttp.ClientSession(headers=self.headers)

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
        """Crawl a single URL and return extracted links and content"""
        domain = urlparse(url).netloc
        
        # Respect robots.txt and crawl delay
        if self.config.get('respect_robots', True) and not self.robots.can_fetch(url):
            logging.info(f"Skipping URL due to robots.txt: {url}")
            return False, [], None
        
        # Respect crawl delay
        self.respect_delay(domain)
        
        # Initialize session if needed
        await self.init_session()
        
        try:
            logging.info(f"Crawling URL: {url}")
            
            # Request the URL
            async with self.session.get(url, timeout=30) as response:
                if response.status != 200:
                    logging.warning(f"Failed to fetch URL: {url}, status: {response.status}")
                    return False, [], None
                
                # Get content type
                content_type = response.headers.get('Content-Type', '')
                if not content_type.startswith('text/html'):
                    logging.info(f"Skipping non-HTML content: {url}, type: {content_type}")
                    return True, [], None
                
                # Get HTML content
                html = await response.text()
                
                # Extract links
                links = self.extract_links(url, html)
                
                # Extract relevant content
                content = self.extract_content(url, html)
                
                # Save HTML content
                self.save_html_content(url, html)
                
                # Update last crawl time
                self.last_crawl_time[domain] = time.time()
                
                return True, links, content
                
        except asyncio.TimeoutError:
            logging.warning(f"Timeout while crawling URL: {url}")
        except Exception as e:
            logging.error(f"Error crawling URL: {url}, error: {e}")
        
        return False, [], None

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

    def send_result(self, url, links, content, success=True, depth=0):
        """Send crawl result back to the master"""
        result = {
            'url': url,
            'links': links,
            'success': success,
            'timestamp': datetime.now().isoformat(),
            'depth': depth
        }
        
        # Only include content if it was extracted successfully
        if content:
            result['content'] = content
        
        if self.gcp_available:
            try:
                # Send to Pub/Sub
                message_json = json.dumps(result).encode('utf-8')
                future = self.publisher.publish(
                    self.results_topic_name, 
                    data=message_json,
                    url=url
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
            message = json.dumps({
                'worker_id': self.worker_id,
                'component': 'crawler',
                'timestamp': datetime.now().isoformat(),
                'status': 'running'
            }).encode('utf-8')
            
            future = self.publisher.publish(
                self.heartbeat_topic_name, 
                data=message,
                worker_id=self.worker_id
            )
            future.result()
            logging.debug("Sent heartbeat")
            return True
        except Exception as e:
            logging.error(f"Failed to send heartbeat: {e}")
            return False

    async def process_message(self, message):
        """Process a crawl request message"""
        try:
            if isinstance(message, str):
                # Message from local file
                data = json.loads(message)
            else:
                # Message from Pub/Sub
                data = json.loads(message.data.decode('utf-8'))
            
            url = data.get('url')
            depth = data.get('depth', 0)
            
            if not url:
                logging.warning("Received message without URL")
                if not isinstance(message, str):
                    message.ack()
                return
            
            # Crawl the URL
            success, links, content = await self.crawl_url(url, depth)
            
            # Send result back to master
            self.send_result(url, links, content, success, depth)
            
            # Acknowledge the message if from Pub/Sub
            if not isinstance(message, str):
                message.ack()
                
            logging.info(f"Processed URL: {url}, success: {success}, links: {len(links) if links else 0}")
            
        except Exception as e:
            logging.error(f"Error processing message: {e}")
            # Don't ack the message to allow retry if it's from Pub/Sub
            if not isinstance(message, str):
                try:
                    message.nack()
                except:
                    pass

    def check_local_tasks(self):
        """Check for crawl tasks in the local file system"""
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
                    success, links, content = await self.crawl_url(url)
                    self.send_result(url, links, content, success)
            
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
                    response = self.subscriber.pull(
                        request={
                            "subscription": self.subscription_name,
                            "max_messages": 5,
                        }
                    )
                    
                    if response.received_messages:
                        for received_message in response.received_messages:
                            await self.process_message(received_message.message)
                
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