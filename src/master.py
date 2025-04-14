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

# Import necessary libraries for task queue, database, etc. (e.g., redis,
# cloud storage SDKs)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - Master - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('master.log')
    ]
)

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
            'queue_size': len(state.url_queue),
            'in_progress': len(state.in_progress),
            'completed': len(state.completed),
            'failed': len(state.failed),
            'worker_stats': state.worker_stats,
            'domain_counts': dict(state.domain_counts),
            'last_update': datetime.now().isoformat(),
            'is_running': True
        }
        with open(state_file, 'w') as f:
            json.dump(current_state, f)
    except Exception as e:
        logging.error(f"Error saving current state: {e}")

def signal_handler(signum, frame):
    """Handle termination signals"""
    global state
    logging.info(f"Received signal {signum}, initiating shutdown...")
    save_current_state(state)
    sys.exit(0)

# Register signal handlers
signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)

def master_process():
    """
    Main process for the master node.
    Handles task distribution, worker management, and coordination.
    """
    global state
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    status = MPI.Status()
    
    # Load configuration from web interface
    try:
        with open('config/crawl_config.json', 'r') as f:
            config = json.load(f)
            seed_urls = config['seed_urls']
            max_depth = config['max_depth']
            max_pages_per_domain = config['max_pages_per_domain']
            respect_robots = config['respect_robots']
            crawl_delay = config['crawl_delay']
            allowed_domains = config['allowed_domains']
            logging.info(f"Loaded configuration: {config}")
    except Exception as e:
        logging.warning(f"Could not load config, using defaults: {e}")
        seed_urls = ["http://example.com"]
        max_depth = 3
        max_pages_per_domain = 1000
        respect_robots = True
        crawl_delay = 1.0
        allowed_domains = None

    # Initialize crawl state with configuration
    state = CrawlState()
    state.config = {
        'max_depth': max_depth,
        'max_pages_per_domain': max_pages_per_domain,
        'respect_robots': respect_robots,
        'crawl_delay': crawl_delay,
        'allowed_domains': allowed_domains
    }
    
    for url in seed_urls:
        if url not in state.seen_urls:
            state.url_queue.append(url)
            state.seen_urls.add(url)
            domain = urlparse(url).netloc
            state.domain_counts[domain] += 1

    monitor = WorkerMonitor()
    
    # Calculate worker distribution
    crawler_nodes = size - 2  # Assuming master and at least one indexer
    indexer_nodes = 1
    
    if crawler_nodes <= 0 or indexer_nodes <= 0:
        logging.error("Not enough nodes. Need at least 3 nodes (1 master, 1 crawler, 1 indexer)")
        return

    active_crawler_nodes = list(range(1, 1 + crawler_nodes))
    active_indexer_nodes = list(range(1 + crawler_nodes, size))
    
    logging.info(f"Active Crawler Nodes: {active_crawler_nodes}")
    logging.info(f"Active Indexer Nodes: {active_indexer_nodes}")

    def handle_crawler_failure(failed_rank):
        """Handle crawler node failure by reassigning its tasks"""
        logging.warning(f"Handling failure of crawler {failed_rank}")
        # Reassign in-progress URLs from failed crawler
        for url, (worker_rank, _) in list(state.in_progress.items()):
            if worker_rank == failed_rank:
                state.url_queue.appendleft(url)  # Priority re-queue
                del state.in_progress[url]
                logging.info(f"Re-queued URL {url} from failed crawler {failed_rank}")

    def check_worker_health():
        """Periodically check worker health"""
        while True:
            monitor.check_workers()
            for rank in monitor.failed_workers - set(state.worker_stats.keys()):
                handle_crawler_failure(rank)
                state.worker_stats[rank]['status'] = 'failed'
            time.sleep(5)  # Check every 5 seconds

    # Start worker health monitoring thread
    health_thread = threading.Thread(target=check_worker_health)
    health_thread.daemon = True
    health_thread.start()

    def assign_url_to_crawler(url, crawler_rank):
        """Assign a URL to a crawler node"""
        state.in_progress[url] = (crawler_rank, datetime.now())
        comm.send(url, dest=crawler_rank, tag=0)
        logging.info(f"Assigned URL {url} to crawler {crawler_rank}")

    # Start periodic status update thread
    def update_status_periodically():
        while True:
            save_current_state(state)
            time.sleep(2)  # Update every 2 seconds

    status_thread = threading.Thread(target=update_status_periodically)
    status_thread.daemon = True
    status_thread.start()

    try:
        while state.url_queue or state.in_progress:
            # Check for messages from workers
            if comm.Iprobe(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status):
                source_rank = status.Get_source()
                tag = status.Get_tag()
                message = comm.recv(source=source_rank, tag=tag)

                # Update worker heartbeat
                if source_rank in active_crawler_nodes:
                    monitor.update_heartbeat(source_rank)

                if tag == 1:  # New URLs discovered
                    new_urls = message
                    domain_limits = defaultdict(lambda: 1000)  # Limit pages per domain
                    
                    for url in new_urls:
                        if url not in state.seen_urls:
                            domain = urlparse(url).netloc
                            if state.domain_counts[domain] < domain_limits[domain]:
                                state.url_queue.append(url)
                                state.seen_urls.add(url)
                                state.domain_counts[domain] += 1

                elif tag == 99:  # Status update
                    if isinstance(message, dict):
                        url = message.get('url')
                        if url in state.in_progress:
                            del state.in_progress[url]
                            state.completed.add(url)
                            state.worker_stats[source_rank]['urls_processed'] += 1

                elif tag == 999:  # Error report
                    if isinstance(message, str):
                        logging.error(f"Error from worker {source_rank}: {message}")
                        # Handle failed URL
                        for url, (worker_rank, _) in list(state.in_progress.items()):
                            if worker_rank == source_rank:
                                state.failed.add(url)
                                del state.in_progress[url]

            # Assign new tasks to available crawlers
            for crawler_rank in active_crawler_nodes:
                if monitor.is_worker_alive(crawler_rank):
                    while state.url_queue and sum(1 for _, (r, _) in state.in_progress.items() if r == crawler_rank) < 5:
                        url = state.url_queue.popleft()
                        if url not in state.in_progress and url not in state.completed:
                            assign_url_to_crawler(url, crawler_rank)

            # Periodic status logging
            if time.time() % 60 < 1:  # Log every minute
                logging.info(f"Status: Queue={len(state.url_queue)}, "
                           f"In Progress={len(state.in_progress)}, "
                           f"Completed={len(state.completed)}, "
                           f"Failed={len(state.failed)}")

            time.sleep(0.1)  # Prevent busy waiting

    except KeyboardInterrupt:
        logging.info("Received keyboard interrupt, shutting down...")
    except Exception as e:
        logging.error(f"Error in master process: {e}")
    finally:
        # Ensure final state is saved
        if 'state' in locals():
            save_current_state(state)

if __name__ == '__main__':
    try:
        master_process()
    except KeyboardInterrupt:
        logging.info("Received keyboard interrupt, shutting down...")
    except Exception as e:
        logging.error(f"Error in master process: {e}")
    finally:
        # Ensure final state is saved
        if 'state' in locals():
            save_current_state(state)