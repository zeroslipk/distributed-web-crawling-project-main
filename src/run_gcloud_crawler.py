import os
import sys
import logging
import logging.handlers
import time
import json
import argparse

def setup_logging(component_type):
    """Setup logging for each component type"""
    log_file = f"gcp{component_type.lower()}.log"
    
    # Close any existing handlers
    for handler in logging.getLogger().handlers[:]:
        handler.close()
        logging.getLogger().removeHandler(handler)
    
    handlers = [logging.StreamHandler()]
    try:
        handlers.append(logging.FileHandler(log_file, mode='w'))
    except PermissionError as e:
        fallback_log = f"gcp{component_type.lower()}_fallback.log"
        print(f"Permission denied for {log_file}: {e}. Falling back to {fallback_log}")
        handlers.append(logging.FileHandler(fallback_log, mode='w'))
    
    logging.basicConfig(
        level=logging.INFO,
        format=f'%(asctime)s - {component_type} - %(levelname)s - %(message)s',
        handlers=handlers
    )

def check_google_credentials():
    """Check if Google Cloud credentials are properly configured"""
    # First check for environment variable
    cred_file = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if not cred_file:
        # Look for a default location in config directory
        default_cred_path = os.path.abspath("config/distributed_crawler.json")
        if os.path.exists(default_cred_path):
            # Using raw string to avoid escape sequence issues
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"{}".format(default_cred_path)
            cred_file = default_cred_path
            print(f"Using credentials file: {cred_file}")
        else:
            print("Warning: Google Cloud credentials not found. Will use local fallback mode.")
            return False
    
    if not os.path.exists(cred_file):
        print(f"Warning: Google Cloud credentials file not found at {cred_file}")
        print("Will use local fallback mode.")
        return False
    
    try:
        with open(cred_file, 'r') as f:
            creds = json.load(f)
            if not creds.get('project_id'):
                print("Warning: Invalid credentials file, project_id not found")
                print("Will use local fallback mode.")
                return False
        
        # The file exists and has a project_id
        print(f"Using Google Cloud project: {creds.get('project_id')}")
        return True
    except json.JSONDecodeError:
        print(f"Warning: The credentials file {cred_file} is not valid JSON")
        print("Will use local fallback mode.")
        return False
    except Exception as e:
        print(f"Warning when checking credentials: {e}")
        print("Will use local fallback mode.")
        return False

def create_lock_file():
    """Create a lock file to prevent multiple instances"""
    lock_file = 'crawler.lock'
    if os.path.exists(lock_file):
        with open(lock_file, 'r') as f:
            pid = f.read().strip()
        print(f"Another instance of the crawler (PID {pid}) may be running. Please terminate it first.")
        return None
    
    # Create lock file with current PID
    with open(lock_file, 'w') as f:
        f.write(str(os.getpid()))
    return lock_file

def run_master(local_mode=False):
    """Run the master component"""
    setup_logging('Master')
    from gcloud_master import main
    main(local_mode=local_mode)

def run_crawler(local_mode=False):
    """Run the crawler component"""
    setup_logging('Crawler')
    from gcloud_crawler import main
    main(local_mode=local_mode)

def run_indexer(local_mode=False):
    """Run the indexer component"""
    setup_logging('Indexer')
    from gcloud_indexer import main
    main(local_mode=local_mode)

def ensure_data_directories():
    """Ensure necessary data directories exist for local mode"""
    os.makedirs('data/html', exist_ok=True)
    os.makedirs('data/state', exist_ok=True)
    os.makedirs('data/index', exist_ok=True)
    os.makedirs('data/crawled', exist_ok=True)
    os.makedirs('config', exist_ok=True)
    
    # Create default configuration if not exists
    config_file = 'config/crawl_config.json'
    if not os.path.exists(config_file):
        default_config = {
            'seed_urls': ['https://example.com'],
            'max_depth': 3,
            'max_pages_per_domain': 25,
            'respect_robots': True,
            'crawl_delay': 1.0,
            'allowed_domains': []
        }
        with open(config_file, 'w') as f:
            json.dump(default_config, f, indent=2)
        print(f"Created default configuration in {config_file}")

def main():
    parser = argparse.ArgumentParser(description='Run Google Cloud-based distributed web crawler components')
    parser.add_argument('component', choices=['master', 'crawler', 'indexer', 'all'],
                        help='Which component to run (master, crawler, indexer, or all)')
    parser.add_argument('--local', action='store_true', 
                        help='Run in local mode without Google Cloud services')
    
    args = parser.parse_args()
    
    # Add src directory to Python path
    src_dir = os.path.dirname(os.path.abspath(__file__))
    sys.path.append(src_dir)
    
    # Set local mode if explicitly requested or if credentials check fails
    local_mode = args.local or not check_google_credentials()
    
    if local_mode:
        print("Running in local mode without Google Cloud services")
        ensure_data_directories()
    
    # Create lock file if running all components
    lock_file = None
    if args.component == 'all':
        lock_file = create_lock_file()
        if not lock_file:
            sys.exit(1)
    
    try:
        if args.component == 'master':
            run_master(local_mode=local_mode)
        elif args.component == 'crawler':
            run_crawler(local_mode=local_mode)
        elif args.component == 'indexer':
            run_indexer(local_mode=local_mode)
        elif args.component == 'all':
            # Import multiprocessing here to avoid circular imports
            import multiprocessing
            
            # Start each component in a separate process
            master_process = multiprocessing.Process(target=run_master, args=(local_mode,))
            crawler_process = multiprocessing.Process(target=run_crawler, args=(local_mode,))
            indexer_process = multiprocessing.Process(target=run_indexer, args=(local_mode,))
            
            master_process.start()
            time.sleep(5)  # Wait a bit for master to initialize
            crawler_process.start()
            indexer_process.start()
            
            # Wait for all processes to finish
            master_process.join()
            crawler_process.join()
            indexer_process.join()
    finally:
        # Remove lock file if created
        if lock_file and os.path.exists(lock_file):
            try:
                os.remove(lock_file)
                print("Removed lock file")
            except Exception as e:
                print(f"Could not remove lock file {lock_file}: {e}")

if __name__ == '__main__':
    main() 