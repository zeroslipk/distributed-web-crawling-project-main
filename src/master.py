from mpi4py import MPI
import time
import logging
import json
from datetime import datetime, timedelta
from collections import defaultdict, deque
from urllib.parse import urlparse
import signal
import threading
from queue import Queue, Empty
import os
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - Master - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('master.log', mode='w')
    ]
)

# Initialize MPI communicator
comm = MPI.COMM_WORLD

class CrawlState:
    def __init__(self):
        self.url_queue = deque()  # URLs to crawl
        self.in_progress = {}  # URL -> (worker_rank, start_time)
        self.completed = set()  # Successfully crawled URLs
        self.failed = set()  # Failed URLs
        self.seen_urls = set()  # All URLs seen (to prevent duplicates)
        self.domain_counts = defaultdict(int)  # Domain -> count of pages crawled
        self.worker_stats = defaultdict(lambda: {
            'urls_processed': 0,
            'last_heartbeat': None,
            'status': 'active'
        })
        self.config = None
        self.available_workers = set()  # Set of available worker ranks
        self.worker_tasks = {}  # worker_rank -> current_url
        self.last_save_time = time.time()  # Initialize last save time
        self.in_progress_lock = threading.Lock()  # Lock for in_progress dictionary

    def should_crawl_domain(self, domain):
        """Check if domain is allowed based on configuration"""
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
        """Check if domain has not reached max_pages_per_domain"""
        return self.domain_counts[domain] < self.config['max_pages_per_domain']

class WorkerMonitor:
    def __init__(self, timeout=30):
        self.timeout = timeout
        self.workers = {}  # rank -> last_heartbeat_time
        self.failed_workers = set()
        self.lock = threading.Lock()

    def update_heartbeat(self, rank, status='active'):
        with self.lock:
            self.workers[rank] = {
                'last_heartbeat': datetime.now(),
                'status': status
            }

    def check_workers(self):
        now = datetime.now()
        with self.lock:
            for rank, info in list(self.workers.items()):
                if (now - info['last_heartbeat']).seconds > self.timeout and info['status'] == 'active':
                    self.failed_workers.add(rank)
                    info['status'] = 'failed'
                    logging.warning(f"Worker {rank} appears to have failed")

    def is_worker_alive(self, rank):
        with self.lock:
            return rank in self.workers and self.workers[rank]['status'] == 'active'

def save_current_state(state):
    """Save current crawl state to file"""
    state_file = 'crawl_state.json'
    try:
        current_state = {
            'queue': list(state.url_queue),
            'in_progress': list(state.in_progress.keys()),
            'completed': list(state.completed),
            'failed': list(state.failed),
            'seen_urls': list(state.seen_urls),
            'domain_counts': dict(state.domain_counts),
            'timestamp': datetime.now().isoformat(),
            'is_running': True
        }
        with open(state_file, 'w') as f:
            json.dump(current_state, f)
    except Exception as e:
        logging.error(f"Error saving current state: {e}")

def cleanup_state():
    """Clean up all state and files"""
    try:
        # Close logging handlers
        for handler in logging.getLogger().handlers[:]:
            handler.close()
            logging.getLogger().removeHandler(handler)
        logging.shutdown()
        
        # Delete state files
        if os.path.exists('crawl_state.json'):
            os.remove('crawl_state.json')
        for log_file in ['master.log']:
            if os.path.exists(log_file):
                try:
                    os.remove(log_file)
                    logging.info(f"Deleted {log_file}")
                except Exception as e:
                    logging.error(f"Could not delete {log_file}: {e}")
        
        # Clear all state
        if 'state' in globals():
            state.url_queue.clear()
            state.in_progress.clear()
            state.completed.clear()
            state.failed.clear()
            state.seen_urls.clear()
            state.domain_counts.clear()
            state.worker_stats.clear()
            state.worker_tasks.clear()
            state.available_workers.clear()
        
        logging.info("Cleaned up all state and files")
    except Exception as e:
        logging.error(f"Error during cleanup: {e}")

def signal_handler(signum, frame):
    """Handle termination signals"""
    global state
    logging.info(f"Received signal {signum}, initiating cleanup and shutdown...")
    # Send shutdown to workers
    for rank in range(1, comm.Get_size()):
        try:
            comm.send({'command': 'shutdown'}, dest=rank, tag=0)
        except:
            pass
    cleanup_state()
    sys.exit(0)

signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)

def assign_url_to_crawler(url, crawler_rank):
    with state.in_progress_lock:
        state.in_progress[url] = (crawler_rank, time.time())
    state.worker_tasks[crawler_rank] = url
    state.available_workers.discard(crawler_rank)
    comm.send({'url': url, 'depth': 0}, dest=crawler_rank, tag=0)
    logging.info(f"URL {url} assigned to crawler {crawler_rank}, in_progress size: {len(state.in_progress)}")

def check_worker_health():
    """Check health of all workers and handle failures"""
    current_time = time.time()
    for rank in list(state.available_workers):
        if rank not in state.worker_stats:
            continue
        last_heartbeat = state.worker_stats[rank]['last_heartbeat']
        if last_heartbeat and (current_time - last_heartbeat) > monitor.timeout:
            logging.warning(f"Worker {rank} appears to have failed")
            handle_crawler_failure(rank)

def handle_crawler_failure(failed_rank):
    """Handle a failed crawler node"""
    if failed_rank in state.worker_tasks:
        failed_url = state.worker_tasks[failed_rank]
        if failed_url in state.in_progress:
            del state.in_progress[failed_url]
        state.failed.add(failed_url)
        state.url_queue.append(failed_url)  # Requeue the URL
        del state.worker_tasks[failed_rank]
    
    state.available_workers.discard(failed_rank)
    state.worker_stats[failed_rank]['status'] = 'failed'

def prune_queue_and_in_progress(state):
    """Remove URLs from queue and in_progress if domain limit is reached"""
    # Prune url_queue
    pruned_queue = deque()
    while state.url_queue:
        url = state.url_queue.popleft()
        domain = urlparse(url).netloc
        if state.can_crawl_domain(domain) and state.should_crawl_domain(domain):
            pruned_queue.append(url)
        else:
            state.seen_urls.add(url)
            logging.info(f"Pruned URL {url} from queue (domain {domain} limit reached or not allowed)")
    state.url_queue = pruned_queue

    # Prune in_progress
    with state.in_progress_lock:
        for url in list(state.in_progress.keys()):
            domain = urlparse(url).netloc
            if not state.can_crawl_domain(domain) or not state.should_crawl_domain(domain):
                worker_rank, _ = state.in_progress[url]
                del state.in_progress[url]
                if worker_rank in state.worker_tasks:
                    del state.worker_tasks[worker_rank]
                state.available_workers.add(worker_rank)
                state.failed.add(url)
                logging.info(f"Pruned URL {url} from in_progress (domain {domain} limit reached or not allowed)")

def master_process():
    """
    Main process for the master node.
    Handles task distribution, worker management, and coordination.
    """
    global state, comm
    rank = comm.Get_rank()
    size = comm.Get_size()
    status = MPI.Status()
    
    # Initialize state
    state = CrawlState()
    monitor = WorkerMonitor()
    
    # Initialize available workers (all ranks except master and indexer)
    state.available_workers = set(range(1, size - 1))
    
    # Load configuration
    try:
        config_path = os.path.abspath('config/crawl_config.json')
        logging.info(f"Attempting to load configuration from {config_path}")
        with open(config_path, 'r') as f:
            config = json.load(f)
            seed_urls = config['seed_urls']
            max_depth = config['max_depth']
            max_pages_per_domain = config['max_pages_per_domain']
            respect_robots = config['respect_robots']
            crawl_delay = config['crawl_delay']
            allowed_domains = config['allowed_domains']
            logging.info(f"Loaded configuration: {config}")
    except FileNotFoundError:
        logging.error(f"Configuration file not found at {config_path}")
        sys.exit(1)
    except Exception as e:
        logging.warning(f"Could not load config, using defaults: {e}")
        seed_urls = ["http://example.com"]
        max_depth = 3
        max_pages_per_domain = 1000
        respect_robots = True
        crawl_delay = 1.0
        allowed_domains = None

    # Initialize crawl state with configuration
    state.config = {
        'max_depth': max_depth,
        'max_pages_per_domain': max_pages_per_domain,
        'respect_robots': respect_robots,
        'crawl_delay': crawl_delay,
        'allowed_domains': allowed_domains
    }
    
    # Clear all existing state
    state.url_queue.clear()
    state.in_progress.clear()
    state.completed.clear()
    state.failed.clear()
    state.seen_urls.clear()
    state.domain_counts.clear()
    state.worker_stats.clear()
    state.worker_tasks.clear()
    logging.info("Cleared all existing crawl state")
    
    # Add seed URLs to queue, respecting max_pages_per_domain
    valid_urls = 0
    for url in seed_urls:
        if url not in state.seen_urls:
            domain = urlparse(url).netloc
            if state.should_crawl_domain(domain):
                if state.can_crawl_domain(domain):
                    state.url_queue.append(url)
                    state.seen_urls.add(url)
                    state.domain_counts[domain] += 1
                    valid_urls += 1
                    logging.info(f"Added seed URL {url} for domain {domain}")
                else:
                    logging.info(f"Skipped seed URL {url} (domain {domain} has reached max_pages_per_domain: {state.domain_counts[domain]}/{state.config['max_pages_per_domain']})")
            else:
                logging.info(f"Skipped seed URL {url} (domain {domain} not in allowed_domains: {state.config['allowed_domains']})")
        else:
            logging.info(f"Skipped seed URL {url} (already seen)")

    # Check if there are any valid URLs to crawl
    if not state.url_queue and not state.in_progress:
        logging.error("No valid seed URLs available to crawl. Check allowed_domains and seed URLs in config.")
        # Send shutdown to workers
        for rank in range(1, size):
            try:
                comm.send({'command': 'shutdown'}, dest=rank, tag=0)
            except:
                pass
        cleanup_state()
        sys.exit(1)

    # Main loop
    while state.url_queue or state.in_progress:
        # Prune queue and in_progress to respect domain limits
        prune_queue_and_in_progress(state)

        # Log current state
        logging.info(f"Queue size: {len(state.url_queue)}, In progress: {len(state.in_progress)}, Completed: {len(state.completed)}, Failed: {len(state.failed)}")

        # Check for messages from workers
        if comm.Iprobe(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status):
            source_rank = status.Get_source()
            tag = status.Get_tag()
            message = comm.recv(source=source_rank, tag=tag)

            # Update worker heartbeat
            if source_rank in state.available_workers:
                monitor.update_heartbeat(source_rank)
                state.worker_stats[source_rank]['last_heartbeat'] = time.time()

            if tag == 1:  # New URLs discovered
                new_urls = message
                for url_dict in new_urls:
                    url = url_dict['url']
                    if url not in state.seen_urls:
                        domain = urlparse(url).netloc
                        if (state.can_crawl_domain(domain) and 
                            state.should_crawl_domain(domain)):
                            state.url_queue.append(url)
                            state.seen_urls.add(url)
                            logging.info(f"Queued new URL {url} for domain {domain}")
                        else:
                            state.seen_urls.add(url)
                            logging.info(f"Skipped URL {url} (domain {domain} limit reached or not allowed)")

            elif tag == 99:  # Status update
                if isinstance(message, dict):
                    url = message.get('url')
                    logging.info(f"Received status update for URL {url} from crawler {source_rank}")
                    with state.in_progress_lock:
                        if url in state.in_progress:
                            del state.in_progress[url]
                            state.completed.add(url)
                            state.worker_stats[source_rank]['urls_processed'] += 1
                            state.available_workers.add(source_rank)
                            if source_rank in state.worker_tasks:
                                del state.worker_tasks[source_rank]
                            domain = urlparse(url).netloc
                            state.domain_counts[domain] += 1
                            logging.info(f"URL {url} marked as completed, completed count: {len(state.completed)}, domain {domain} count: {state.domain_counts[domain]}")
                        else:
                            logging.warning(f"URL {url} not found in in_progress dictionary")

            elif tag == 999:  # Error message
                logging.error(f"Error from worker {source_rank}: {message}")
                if source_rank in state.worker_tasks:
                    failed_url = state.worker_tasks[source_rank]
                    state.failed.add(failed_url)
                    if failed_url in state.in_progress:
                        del state.in_progress[failed_url]
                    state.available_workers.add(source_rank)
                    del state.worker_tasks[source_rank]

        # Assign URLs to available workers
        while state.url_queue and state.available_workers:
            url = state.url_queue[0]
            domain = urlparse(url).netloc
            if state.can_crawl_domain(domain) and state.should_crawl_domain(domain):
                url = state.url_queue.popleft()
                worker_rank = min(state.available_workers)
                assign_url_to_crawler(url, worker_rank)
            else:
                state.url_queue.popleft()
                state.seen_urls.add(url)
                logging.info(f"Removed URL {url} from queue (domain {domain} limit reached or not allowed)")

        # Check worker health
        check_worker_health()

        # Save state periodically
        if time.time() - state.last_save_time > 60:
            save_current_state(state)
            state.last_save_time = time.time()

        time.sleep(0.1)

    # Send shutdown signal to all workers
    for rank in range(1, size):
        try:
            comm.send({'command': 'shutdown'}, dest=rank, tag=0)
        except:
            pass
    
    # Clean up before exiting
    cleanup_state()

if __name__ == '__main__':
    try:
        master_process()
    except KeyboardInterrupt:
        logging.info("Received keyboard interrupt, shutting down...")
        cleanup_state()
    except Exception as e:
        logging.error(f"Error in master process: {e}")
        cleanup_state()
    finally:
        if 'state' in locals():
            cleanup_state()