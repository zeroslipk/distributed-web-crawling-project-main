import os
import time
import logging
import json
from datetime import datetime
from collections import defaultdict, deque
from urllib.parse import urlparse
import threading
import uuid

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - GCP Master - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('gcpmaster.log', mode='w')
    ]
)

# Late imports for Google Cloud services to allow local mode without GCP dependencies
google_cloud_available = True
try:
    from google.cloud import pubsub_v1, storage
except ImportError:
    google_cloud_available = False
    logging.warning("Google Cloud libraries not available. Will run in local-only mode.")

class CrawlState:
    def __init__(self):
        self.url_queue = deque()
        self.in_progress = {}
        self.completed = set()
        self.failed = set()
        self.seen_urls = set()
        self.domain_counts = defaultdict(int)
        self.worker_stats = defaultdict(lambda: {
            'urls_processed': 0,
            'last_heartbeat': None,
            'status': 'active'
        })
        self.config = None
        self.last_save_time = time.time()
        self.in_progress_lock = threading.Lock()
        self.recently_added = []  # Track recently added URLs
        self.recently_added_time = time.time()  # When URLs were last added
        self.failure_reasons = {}  # Track reasons for URL failures
        self.retry_counters = {}  # Track retry attempts for URLs

    def should_crawl_domain(self, domain):
        allowed_domains = self.config.get('allowed_domains')
        if not allowed_domains or not any(allowed_domains):
            return True
        normalized_domain = domain.lower()
        return any(
            normalized_domain == allowed.lower() or 
            normalized_domain.endswith('.' + allowed.lower())
            for allowed in allowed_domains
        )

    def can_crawl_domain(self, domain):
        return self.domain_counts[domain] < self.config['max_pages_per_domain']

class GCPMaster:
    def __init__(self, project_id=None, topic_name_prefix='web-crawler', local_mode=False):
        self.project_id = project_id
        self.local_mode = local_mode or not google_cloud_available
        
        # Initialize clients based on mode
        if not self.local_mode and google_cloud_available:
            try:
                self.publisher = pubsub_v1.PublisherClient()
                self.storage_client = storage.Client()
                self.bucket_name = f"{project_id}-crawler-data"
                
                # Topic and subscription names
                self.crawler_topic_name = f"projects/{project_id}/topics/{topic_name_prefix}-crawl-requests"
                self.indexer_topic_name = f"projects/{project_id}/topics/{topic_name_prefix}-index-requests"
                self.results_topic_name = f"projects/{project_id}/topics/{topic_name_prefix}-results"
                self.heartbeat_topic_name = f"projects/{project_id}/topics/{topic_name_prefix}-heartbeats"
                
                # Create topics if they don't exist
                self._create_topics()
                self._create_bucket_if_not_exists()
                self.using_gcp = True
                logging.info("GCP Master initialized with Google Cloud services")
            except Exception as e:
                logging.error(f"Failed to initialize Google Cloud services: {e}")
                self.local_mode = True
                self.using_gcp = False
        else:
            logging.info("Running in local-only mode")
            self.using_gcp = False
        
        # Create local directories for storage in local mode
        if self.local_mode:
            os.makedirs('data/html', exist_ok=True)
            os.makedirs('data/state', exist_ok=True)
            os.makedirs('data/config', exist_ok=True)
            os.makedirs('data/index', exist_ok=True)
            os.makedirs('data/urls', exist_ok=True)
        
        # Load config
        self.load_config()
        
        # Create state for local management
        self.state = CrawlState()
        self.state.config = self.config

    def _create_topics(self):
        """Create Pub/Sub topics if they don't exist"""
        topics = [
            self.crawler_topic_name,
            self.indexer_topic_name,
            self.results_topic_name,
            self.heartbeat_topic_name
        ]
        
        for topic in topics:
            try:
                self.publisher.get_topic(request={"topic": topic})
                logging.info(f"Topic {topic} already exists")
            except Exception:
                try:
                    topic_obj = self.publisher.create_topic(request={"name": topic})
                    logging.info(f"Created topic: {topic_obj.name}")
                except Exception as e:
                    logging.error(f"Failed to create topic {topic}: {e}")
                    logging.info("Will continue without Pub/Sub functionality.")

    def _create_bucket_if_not_exists(self):
        """Create Cloud Storage bucket if it doesn't exist"""
        try:
            bucket = self.storage_client.get_bucket(self.bucket_name)
            logging.info(f"Bucket {self.bucket_name} already exists")
        except Exception:
            try:
                bucket = self.storage_client.create_bucket(self.bucket_name)
                logging.info(f"Created bucket: {bucket.name}")
            except Exception as e:
                logging.error(f"Failed to create bucket: {e}")
                logging.info("Using local storage as fallback")
                # Create local directories as fallback
                os.makedirs('data/html', exist_ok=True)
                os.makedirs('data/state', exist_ok=True)
                os.makedirs('data/config', exist_ok=True)

    def load_config(self):
        """Load crawler configuration from Cloud Storage or local file"""
        if not self.local_mode and self.using_gcp:
            try:
                # Try to get config from Cloud Storage
                bucket = self.storage_client.bucket(self.bucket_name)
                blob = bucket.blob('config/crawler_config.json')
                
                if blob.exists():
                    config_json = blob.download_as_string()
                    self.config = json.loads(config_json)
                    logging.info("Loaded config from Cloud Storage")
                    return
            except Exception as e:
                logging.error(f"Error loading config from Cloud Storage: {e}")
        
        # Try to load from local file
        try:
            with open('config/crawl_config.json', 'r') as f:
                self.config = json.load(f)
            logging.info("Loaded config from local file")
            return
        except:
            # Fallback to default config
            self._set_default_config()

    def _set_default_config(self):
        """Set default configuration"""
        self.config = {
            'seed_urls': ['https://example.com'],
            'max_depth': 3,
            'max_pages_per_domain': 25,
            'respect_robots': True,
            'crawl_delay': 1.0,
            'allowed_domains': []
        }
        logging.info("Using default config")
        
        # Try to save default config to disk
        try:
            os.makedirs('config', exist_ok=True)
            with open('config/crawl_config.json', 'w') as f:
                json.dump(self.config, f, indent=2)
        except Exception as e:
            logging.error(f"Error saving default config to disk: {e}")

    def save_state_to_storage(self):
        """Save the current crawl state to Cloud Storage or local file"""
        # Convert state to a serializable format
        state_data = {
            'in_progress': list(self.state.in_progress.keys()),
            'completed': list(self.state.completed),
            'failed': list(self.state.failed),
            'seen_urls': list(self.state.seen_urls),
            'domain_counts': dict(self.state.domain_counts),
            'timestamp': datetime.now().isoformat(),
            'is_running': True,
            'queue': list(self.state.url_queue),
            'failure_reasons': dict(self.state.failure_reasons)
        }
        
        # Include recently added URLs in the state
        if hasattr(self.state, 'recently_added') and self.state.recently_added:
            # Only keep recently added URLs for 5 minutes
            if time.time() - self.state.recently_added_time < 300:  # 5 minutes
                state_data['recently_added'] = self.state.recently_added
            else:
                # Clear the list if it's been more than 5 minutes
                self.state.recently_added = []

        # Save based on mode
        if not self.local_mode and self.using_gcp:
            try:
                bucket = self.storage_client.bucket(self.bucket_name)
                blob = bucket.blob('state/current_state.json')
                blob.upload_from_string(json.dumps(state_data), content_type='application/json')
                
                # Also save domain statistics
                for domain, count in self.state.domain_counts.items():
                    domain_blob = bucket.blob(f'state/domains/{domain}.json')
                    domain_blob.upload_from_string(json.dumps({
                        'domain': domain,
                        'page_count': count,
                        'last_updated': datetime.now().isoformat()
                    }), content_type='application/json')
                
                logging.debug("Saved state to Cloud Storage")
            except Exception as e:
                logging.error(f"Failed to save state to Cloud Storage: {e}")
                self._save_state_locally(state_data)
        else:
            self._save_state_locally(state_data)

    def _save_state_locally(self, state_data=None):
        """Save state to local file system"""
        if state_data is None:
            state_data = {
                'in_progress': list(self.state.in_progress.keys()),
                'completed': list(self.state.completed),
                'failed': list(self.state.failed),
                'seen_urls': list(self.state.seen_urls),
                'domain_counts': dict(self.state.domain_counts),
                'timestamp': datetime.now().isoformat(),
                'is_running': True,
                'failure_reasons': dict(self.state.failure_reasons)
            }
        
        try:
            os.makedirs('data/state', exist_ok=True)
            with open('data/state/current_state.json', 'w') as f:
                json.dump(state_data, f, indent=2)
            
            # Save domain statistics separately
            os.makedirs('data/state/domains', exist_ok=True)
            for domain, count in self.state.domain_counts.items():
                with open(f'data/state/domains/{domain.replace(":", "_")}.json', 'w') as f:
                    json.dump({
                        'domain': domain,
                        'page_count': count,
                        'last_updated': datetime.now().isoformat()
                    }, f, indent=2)
            
            logging.debug("Saved state locally")
        except Exception as e:
            logging.error(f"Failed to save state locally: {e}")

    def load_state_from_storage(self):
        """Load state from Cloud Storage or local file"""
        if not self.local_mode and self.using_gcp:
            try:
                bucket = self.storage_client.bucket(self.bucket_name)
                blob = bucket.blob('state/current_state.json')
                
                if blob.exists():
                    state_json = blob.download_as_string()
                    state_data = json.loads(state_json)
                    self._update_state_from_data(state_data)
                    logging.info("Loaded state from Cloud Storage")
                    return
            except Exception as e:
                logging.error(f"Error loading state from Cloud Storage: {e}")
        
        # Try loading from local file
        try:
            if os.path.exists('data/state/current_state.json'):
                with open('data/state/current_state.json', 'r') as f:
                    state_data = json.load(f)
                self._update_state_from_data(state_data)
                logging.info("Loaded state from local file")
                return
        except Exception as e:
            logging.error(f"Error loading state from local file: {e}")
        
        logging.info("No previous state found, starting fresh")

    def _update_state_from_data(self, state_data):
        """Update the crawl state from loaded data"""
        with self.state.in_progress_lock:
            self.state.in_progress = {url: (None, time.time()) for url in state_data.get('in_progress', [])}
        self.state.completed = set(state_data.get('completed', []))
        self.state.failed = set(state_data.get('failed', []))
        self.state.seen_urls = set(state_data.get('seen_urls', []))
        self.state.domain_counts = defaultdict(int, state_data.get('domain_counts', {}))
        self.state.failure_reasons = state_data.get('failure_reasons', {})
        
        # Add any in-progress URLs back to the queue if they were being processed
        for url in self.state.in_progress:
            self.state.url_queue.append(url)
        
        # Reset in-progress tracking
        self.state.in_progress = {}

    def add_urls_to_queue(self, urls):
        """Add URLs to the crawl queue"""
        for url_item in urls:
            if isinstance(url_item, tuple):
                url, depth = url_item
            else:
                url, depth = url_item, 0
                
            # Only add if it's not already processed or queued
            if (url not in self.state.seen_urls and 
                url not in self.state.completed and 
                url not in self.state.failed):
                
                # Check domain policy
                domain = urlparse(url).netloc
                if self.state.should_crawl_domain(domain) and self.state.can_crawl_domain(domain):
                    self.state.url_queue.append((url, depth))
                    self.state.seen_urls.add(url)
                    self.state.domain_counts[domain] += 1

    def send_to_crawler(self, url, depth=0):
        """Send a URL to a crawler for processing"""
        domain = urlparse(url).netloc
        message = json.dumps({
            'url': url,
            'depth': depth,
            'timestamp': datetime.now().isoformat(),
            'request_id': str(uuid.uuid4())
        })
        
        # Add to in-progress with timestamp
        with self.state.in_progress_lock:
            self.state.in_progress[url] = time.time()
        
        if not self.local_mode and self.using_gcp:
            try:
                # Send to Pub/Sub
                future = self.publisher.publish(
                    self.crawler_topic_name, 
                    data=message.encode('utf-8'),
                    url=url,
                    depth=str(depth)
                )
                message_id = future.result()
                logging.debug(f"Sent to crawler via Pub/Sub: {url} (ID: {message_id})")
                return True
            except Exception as e:
                logging.error(f"Error sending to Pub/Sub: {e}")
                # Fall back to local file
        
        # If in local mode or Pub/Sub failed, use file-based approach
        try:
            os.makedirs('data/urls/to_crawl', exist_ok=True)
            filename = f'data/urls/to_crawl/{uuid.uuid4()}.json'
            with open(filename, 'w') as f:
                f.write(message)
            logging.debug(f"Sent to crawler via file: {url}")
            return True
        except Exception as e:
            logging.error(f"Error writing URL to file: {e}")
            return False

    def process_results(self, message):
        """Process crawl results received from a crawler"""
        try:
            # Parse message based on source
            if isinstance(message, str):
                # Message from local file
                data = json.loads(message)
                pubsub_message = False
            elif hasattr(message, 'data'):
                # Message from subscription callback
                data = json.loads(message.data.decode('utf-8'))
                pubsub_message = True
            else:
                # Message from Pub/Sub pull
                data = json.loads(message.message.data.decode('utf-8'))
                pubsub_message = True
            
            # Extract data
            url = data.get('url')
            success = data.get('success', False)
            links = data.get('links', [])
            content = data.get('content')
            depth = data.get('depth', 0)
            
            # Initialize failure reasons dict if not exists
            if not hasattr(self.state, 'failure_reasons'):
                self.state.failure_reasons = {}
            
            # Store failure reason if failed
            if not success:
                error = data.get('error', 'Unknown error')
                self.state.failure_reasons[url] = error
            
            # Update state
            with self.state.in_progress_lock:
                if url in self.state.in_progress:
                    # Remove from in-progress
                    del self.state.in_progress[url]
                    
                    # Add to appropriate set based on success
                    if success:
                        self.state.completed.add(url)
                        
                        # Add links to queue if within depth limit
                        if depth < self.config.get('max_depth', 3):
                            self.add_urls_to_queue([(link, depth + 1) for link in links])
                        
                        # Send to indexer if content is available
                        if content and (url not in self.state.failed or url in self.state.in_progress):
                            self.send_to_indexer(url, content)
                    else:
                        self.state.failed.add(url)
            
            # Save state if it's been a while
            if time.time() - self.state.last_save_time > 60:  # Save every minute
                self.save_state_to_storage()
                self.state.last_save_time = time.time()
            
            return True
        except Exception as e:
            logging.error(f"Error processing result: {e}")
            return False

    def send_to_indexer(self, url, content):
        """Send content to the indexer"""
        message = json.dumps({
            'url': url,
            'content': content,
            'timestamp': datetime.now().isoformat(),
            'request_id': str(uuid.uuid4())
        })
        
        if not self.local_mode and self.using_gcp:
            try:
                # Send to Pub/Sub
                future = self.publisher.publish(
                    self.indexer_topic_name, 
                    data=message.encode('utf-8'),
                    url=url
                )
                message_id = future.result()
                logging.debug(f"Sent to indexer via Pub/Sub: {url} (ID: {message_id})")
                return True
            except Exception as e:
                logging.error(f"Error sending to indexer via Pub/Sub: {e}")
                # Fall back to local file
        
        # If in local mode or Pub/Sub failed, use file-based approach
        try:
            os.makedirs('data/urls/to_index', exist_ok=True)
            filename = f'data/urls/to_index/{uuid.uuid4()}.json'
            with open(filename, 'w') as f:
                f.write(message)
            logging.debug(f"Sent to indexer via file: {url}")
            return True
        except Exception as e:
            logging.error(f"Error writing indexing data to file: {e}")
            return False

    def check_local_results(self):
        """Check for crawler results in the file system"""
        result_dir = 'data/urls/crawled'
        if os.path.exists(result_dir):
            for filename in os.listdir(result_dir):
                if filename.endswith('.json'):
                    filepath = os.path.join(result_dir, filename)
                    try:
                        with open(filepath, 'r') as f:
                            result = json.load(f)
                        self.process_results(result)
                        # Remove the file after processing
                        os.remove(filepath)
                    except Exception as e:
                        logging.error(f"Error processing local result file {filepath}: {e}")

    def check_timeouts(self, timeout_seconds=300):
        """Check for URLs that have been in progress for too long"""
        current_time = time.time()
        timed_out_urls = []
        
        with self.state.in_progress_lock:
            for url, start_time in list(self.state.in_progress.items()):
                if current_time - start_time > timeout_seconds:
                    timed_out_urls.append(url)
                    del self.state.in_progress[url]
                    
                    # Check and update retry counter
                    if not hasattr(self.state, 'retry_counters'):
                        self.state.retry_counters = {}
                    
                    retry_count = self.state.retry_counters.get(url, 0) + 1
                    self.state.retry_counters[url] = retry_count
                    
                    # If we've retried too many times, mark as permanently failed
                    max_retries = 3
                    if retry_count > max_retries:
                        self.state.failed.add(url)
                        logging.warning(f"URL timed out and permanently failed after {retry_count} retries: {url}")
                    else:
                        self.state.failed.add(url)
                        logging.warning(f"URL timed out (attempt {retry_count}/{max_retries}): {url}")
        
        # Re-queue timed out URLs that haven't exceeded retry limit
        for url in timed_out_urls:
            if url not in self.state.url_queue and url not in self.state.completed:
                # Only re-queue if under retry limit
                retry_count = self.state.retry_counters.get(url, 0)
                if retry_count <= 3:  # max_retries
                    self.state.url_queue.append(url)
                    logging.info(f"Re-queued timed out URL: {url} (attempt {retry_count}/3)")

    def initialize_crawl(self):
        """Initialize the crawling process with seed URLs"""
        seed_urls = self.config.get('seed_urls', [])
        if seed_urls:
            self.add_urls_to_queue(seed_urls)
            logging.info(f"Initialized crawl with {len(seed_urls)} seed URLs")
        else:
            logging.warning("No seed URLs provided in config")

    def check_for_config_updates(self):
        """Check for config updates and add any new seed URLs to the queue"""
        try:
            new_config = None
            config_updated = False
            
            # Try to load from local file first (this is the most common update path from the web UI)
            try:
                with open('config/crawl_config.json', 'r') as f:
                    new_config = json.load(f)
                    config_updated = True
            except Exception as local_e:
                logging.debug(f"No local config update found: {local_e}")
            
            # Check Cloud Storage if in GCP mode
            if not self.local_mode and self.using_gcp and not config_updated:
                try:
                    bucket = self.storage_client.bucket(self.bucket_name)
                    blob = bucket.blob('config/crawler_config.json')
                    
                    if blob.exists():
                        config_json = blob.download_as_string()
                        new_config = json.loads(config_json)
                        config_updated = True
                except Exception as gcp_e:
                    logging.error(f"Error checking GCP config: {gcp_e}")
            
            # If config was updated, compare seed URLs and add new ones
            if config_updated and new_config:
                current_seed_urls = set(self.config.get('seed_urls', []))
                new_seed_urls = set(new_config.get('seed_urls', []))
                
                # Find URLs that are in the new config but not in current config
                added_urls = new_seed_urls - current_seed_urls
                
                if added_urls:
                    logging.info(f"Found {len(added_urls)} new seed URLs in updated config")
                    
                    # Add new URLs to queue if they haven't been processed
                    new_urls_added = []
                    for url in added_urls:
                        if (url not in self.state.seen_urls and 
                            url not in self.state.completed and 
                            url not in self.state.failed):
                            self.state.url_queue.append((url, 0))  # Start at depth 0
                            self.state.seen_urls.add(url)
                            new_urls_added.append(url)
                            
                            # Update domain counters for URL
                            domain = urlparse(url).netloc
                            if domain and self.state.should_crawl_domain(domain):
                                logging.info(f"Added new seed URL to queue: {url}")
                    
                    if new_urls_added:
                        logging.info(f"Added {len(new_urls_added)} new URLs to crawl queue")
                        # Track newly added URLs for display in UI
                        self.state.recently_added = new_urls_added
                        self.state.recently_added_time = time.time()
                        # Update our config with the new one
                        self.config = new_config
                
                # Check for other config changes
                for key in ['max_depth', 'max_pages_per_domain', 'respect_robots', 'crawl_delay', 'allowed_domains']:
                    if self.config.get(key) != new_config.get(key):
                        logging.info(f"Updated configuration parameter: {key}")
                        self.config = new_config
                        break
                
                # Make sure state has updated config
                self.state.config = self.config
                
                return len(added_urls) > 0
        except Exception as e:
            logging.error(f"Error checking for config updates: {e}")
        
        return False

    def run(self):
        """Main crawling loop"""
        # Load previous state if any
        self.load_state_from_storage()
        
        # Initialize with seed URLs if queue is empty
        if not self.state.url_queue and not self.state.in_progress:
            self.initialize_crawl()
        
        logging.info("Starting crawl process")
        self.state.last_save_time = time.time()
        last_config_check_time = time.time()
        
        # Set up heartbeat thread if using GCP
        if not self.local_mode and self.using_gcp:
            heartbeat_thread = threading.Thread(target=self._send_heartbeat_loop)
            heartbeat_thread.daemon = True
            heartbeat_thread.start()
        
        # Main crawling loop
        try:
            while self.state.url_queue or self.state.in_progress:
                # Check for timeouts
                self.check_timeouts()
                
                # In local mode, check for results from files
                if self.local_mode or not self.using_gcp:
                    self.check_local_results()
                
                # Periodically check for config updates (every 30 seconds)
                current_time = time.time()
                if current_time - last_config_check_time > 30:
                    self.check_for_config_updates()
                    last_config_check_time = current_time
                
                # Process URLs from queue
                if self.state.url_queue:
                    url_info = self.state.url_queue.popleft()
                    if isinstance(url_info, tuple):
                        url, depth = url_info
                    else:
                        url, depth = url_info, 0
                    
                    success = self.send_to_crawler(url, depth)
                    if not success:
                        # Put it back in the queue
                        self.state.url_queue.appendleft((url, depth))
                    
                # Sleep to avoid tight loop
                time.sleep(0.1)
                
                # Save state periodically
                current_time = time.time()
                if current_time - self.state.last_save_time > 60:  # Save every minute
                    self.save_state_to_storage()
                    self.state.last_save_time = current_time
                    
                    # Analyze failure reasons if there are failed URLs
                    failure_counts = {}
                    if hasattr(self.state, 'failure_reasons'):
                        for reason in self.state.failure_reasons.values():
                            if reason in failure_counts:
                                failure_counts[reason] += 1
                            else:
                                failure_counts[reason] = 1
                        
                        failure_summary = ", ".join([f"{count} {reason}" for reason, count in failure_counts.items()])
                        logging.info(f"Stats: Queue={len(self.state.url_queue)}, In Progress={len(self.state.in_progress)}, Completed={len(self.state.completed)}, Failed={len(self.state.failed)}/{failure_summary if failure_summary else ''}")
                    else:
                        logging.info(f"Stats: Queue={len(self.state.url_queue)}, In Progress={len(self.state.in_progress)}, Completed={len(self.state.completed)}, Failed={len(self.state.failed)}")
            
            # Final state save
            self.save_state_to_storage()
            logging.info("Crawling complete")
        except KeyboardInterrupt:
            logging.info("Crawling interrupted, saving state...")
            self.save_state_to_storage()
            logging.info("State saved, exiting")
        except Exception as e:
            logging.error(f"Error in crawl process: {e}")
            self.save_state_to_storage()
            raise

    def _send_heartbeat_loop(self):
        """Send periodic heartbeats to signal this master is alive"""
        while True:
            try:
                message = json.dumps({
                    'component': 'master',
                    'timestamp': datetime.now().isoformat(),
                    'status': 'running',
                    'queue_size': len(self.state.url_queue),
                    'in_progress': len(self.state.in_progress),
                    'completed': len(self.state.completed),
                    'failed': len(self.state.failed)
                })
                
                self.publisher.publish(
                    self.heartbeat_topic_name, 
                    data=message.encode('utf-8')
                )
                logging.debug("Sent heartbeat")
            except Exception as e:
                logging.error(f"Error sending heartbeat: {e}")
            
            # Sleep for 30 seconds before next heartbeat
            time.sleep(30)

def main(local_mode=False):
    # Get project ID from the service account file or environment
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
    
    # Initialize and run the master
    master = GCPMaster(project_id=project_id, local_mode=local_mode)
    master.run()

if __name__ == '__main__':
    main() 