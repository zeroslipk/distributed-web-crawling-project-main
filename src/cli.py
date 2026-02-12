#!/usr/bin/env python3
import argparse
import json
import os
import sys
from mpi4py import MPI
import logging
import time
from urllib.parse import urlparse
import re
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - CLI - %(levelname)s - %(message)s'
)

def validate_domain(domain):
    """Validate that the input is a valid domain (e.g., example.com, not a path or URL)"""
    domain_regex = r'^([a-zA-Z0-9]([a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?\.)+[a-zA-Z]{2,}$'
    return bool(re.match(domain_regex, domain))

def validate_url(url):
    """Validate URL format"""
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except:
        return False

def load_urls_from_file(file_path):
    """Load URLs from a file (one URL per line)"""
    with open(file_path, 'r') as f:
        urls = [line.strip() for line in f if line.strip()]
    return [url for url in urls if validate_url(url)]

def save_urls_to_file(urls, file_path):
    """Save URLs to a file"""
    with open(file_path, 'w') as f:
        for url in urls:
            f.write(f"{url}\n")

def start_crawl(args):
    """Start a new crawl with the given parameters"""
    # Validate seed URLs
    seed_urls = []
    if args.url:
        if validate_url(args.url):
            seed_urls.append(args.url)
        else:
            logging.error(f"Invalid URL format: {args.url}")
            return

    if args.file:
        file_urls = load_urls_from_file(args.file)
        if not file_urls:
            logging.error(f"No valid URLs found in file: {args.file}")
            return
        seed_urls.extend(file_urls)

    if not seed_urls:
        logging.error("No valid seed URLs provided. Please provide at least one valid URL.")
        return

    # Validate allowed domains
    allowed_domains = []
    if args.allowed_domains:
        for domain in args.allowed_domains.split(','):
            domain = domain.strip()
            if validate_domain(domain):
                allowed_domains.append(domain)
            else:
                logging.error(f"Invalid domain format: {domain}. Must be a valid domain (e.g., example.com).")
                return

    # Save configuration
    config = {
        'seed_urls': seed_urls,
        'max_depth': args.depth,
        'max_pages_per_domain': args.limit,
        'respect_robots': not args.ignore_robots,
        'crawl_delay': args.delay,
        'allowed_domains': allowed_domains or None,
        'start_time': time.strftime('%Y-%m-%d %H:%M:%S')
    }

    os.makedirs('config', exist_ok=True)
    with open('config/crawl_config.json', 'w') as f:
        json.dump(config, f, indent=2)

    # Start the crawl using run_gcloud_crawler.py
    num_nodes = args.nodes if args.nodes else 3
    # Note: run_gcloud_crawler.py 'all' handles master, crawler, and indexer
    cmd = f"python src/run_gcloud_crawler.py all --local"
    logging.info(f"Starting crawl with command: {cmd}")
    os.system(cmd)

def show_status(args):
    """Show current crawl status"""
    try:
        state_file = 'data/state/current_state.json'
        if not os.path.exists(state_file):
            logging.error(f"No crawl state found at {state_file}. Has a crawl been started?")
            return

        with open(state_file, 'r') as f:
            state = json.load(f)
        
        print("\nCrawl Status:")
        print("-" * 50)
        queue_size = len(state.get('queue', []))
        completed_size = len(state.get('completed', []))
        failed_size = len(state.get('failed', []))
        seen_size = len(state.get('seen_urls', []))
        
        print(f"Queue Size: {queue_size}")
        print(f"Completed URLs: {completed_size}")
        print(f"Failed URLs: {failed_size}")
        print(f"Total Unique URLs Seen: {seen_size}")
        print(f"Domains Crawled: {len(state.get('domain_counts', {}))}")
        print(f"Last Updated: {state.get('timestamp', 'Unknown')}")
        print("-" * 50)
    except Exception as e:
        logging.error(f"Error reading crawl state: {e}")

def search_index(args):
    """Search the index for the given query (Local Implementation)"""
    index_file = 'data/index/latest_index.json'
    if not os.path.exists(index_file):
        logging.error(f"Index file not found at {index_file}. Crawl some pages first.")
        return

    try:
        with open(index_file, 'r') as f:
            index_data = json.load(f)
        
        # Simple local search logic
        query_terms = re.findall(r'\w+', args.query.lower())
        if not query_terms:
            print("Empty query.")
            return

        scores = {} # url -> score
        url_metadata = {} # url -> {title, timestamp}

        content_index = index_data.get('content_index', {})
        
        for term in query_terms:
            if term in content_index:
                # content_index[term] is a list of [url, score, title, timestamp]
                for entry in content_index[term]:
                    url = entry[0]
                    score = entry[1]
                    title = entry[2]
                    timestamp = entry[3]
                    
                    scores[url] = scores.get(url, 0.0) + score
                    url_metadata[url] = {'title': title, 'timestamp': timestamp}

        # Filter by field if specified (very basic filter)
        results = []
        for url, score in scores.items():
            meta = url_metadata[url]
            if args.field == 'title' and not any(t in meta['title'].lower() for t in query_terms):
                continue
            if args.field == 'url' and not any(t in url.lower() for t in query_terms):
                continue
            
            results.append({
                'url': url,
                'score': score,
                'title': meta['title'],
                'timestamp': meta['timestamp']
            })

        results.sort(key=lambda x: x['score'], reverse=True)
        top_results = results[:args.limit]

        print(f"\nSearch Results for '{args.query}':")
        print("-" * 50)
        if not top_results:
            print("No matches found.")
        else:
            for i, result in enumerate(top_results, 1):
                print(f"\n{i}. {result['title']}")
                print(f"URL: {result['url']}")
                print(f"Score: {result['score']:.2f}")
                print(f"Indexed: {result['timestamp']}")
        print("-" * 50)

    except Exception as e:
        logging.error(f"Error performing local search: {e}")

def reset_failures(args):
    """Reset failed URLs or problematic domains"""
    if args.clear_problematic_domains:
        # Create a flag file for the crawler to clear problematic domains
        with open('clear_problematic_domains.flag', 'w') as f:
            f.write(f"Clear request at {datetime.now().isoformat()}")
        print("Flag created to clear problematic domains. Will be processed on next crawler run.")
    
    if args.reset_failed_urls:
        # Load state file
        state_file = 'data/state/current_state.json'
        if not os.path.exists(state_file):
            print("No state file found. Cannot reset failed URLs.")
            return
        
        try:
            with open(state_file, 'r') as f:
                state = json.load(f)
            
            # Move failed URLs back to queue
            failed_urls = state.get('failed', [])
            queue = state.get('queue', [])
            
            # Add failed URLs to queue if not already there
            for url in failed_urls:
                if url not in queue:
                    queue.append(url)
            
            # Clear failed URLs
            state['failed'] = []
            
            # Save updated state
            with open(state_file, 'w') as f:
                json.dump(state, f, indent=2)
            
            print(f"Reset {len(failed_urls)} failed URLs back to the queue.")
        except Exception as e:
            print(f"Error resetting failed URLs: {e}")

def main():
    parser = argparse.ArgumentParser(description='Distributed Web Crawler CLI')
    subparsers = parser.add_subparsers(dest='command', help='Commands')

    # Start command
    start_parser = subparsers.add_parser('start', help='Start a new crawl')
    start_parser.add_argument('--url', help='Single seed URL to start crawling from')
    start_parser.add_argument('--file', help='File containing seed URLs (one per line)')
    start_parser.add_argument('--depth', type=int, default=3, help='Maximum crawl depth')
    start_parser.add_argument('--limit', type=int, default=1000, help='Maximum pages per domain')
    start_parser.add_argument('--delay', type=float, default=1.0, help='Default crawl delay in seconds')
    start_parser.add_argument('--nodes', type=int, help='Number of nodes to use')
    start_parser.add_argument('--allowed-domains', help='Comma-separated list of allowed domains (e.g., example.com)')
    start_parser.add_argument('--ignore-robots', action='store_true', help='Ignore robots.txt')

    # Status command
    status_parser = subparsers.add_parser('status', help='Show crawl status')

    # Search command
    search_parser = subparsers.add_parser('search', help='Search the index')
    search_parser.add_argument('query', help='Search query')
    search_parser.add_argument('--field', default='content', choices=['content', 'title', 'url'],
                             help='Field to search in')
    search_parser.add_argument('--limit', type=int, default=10, help='Maximum number of results')

    # Reset failed URLs command
    reset_parser = subparsers.add_parser('reset-failures', help='Reset failed URLs or problematic domains')
    reset_parser.add_argument('--clear-problematic-domains', action='store_true', help='Clear the list of problematic domains')
    reset_parser.add_argument('--reset-failed-urls', action='store_true', help='Reset all failed URLs back to the queue')
    reset_parser.set_defaults(func=reset_failures)

    args = parser.parse_args()

    if args.command == 'start':
        start_crawl(args)
    elif args.command == 'status':
        show_status(args)
    elif args.command == 'search':
        search_index(args)
    elif args.command == 'reset-failures':
        reset_failures(args)
    else:
        parser.print_help()

if __name__ == '__main__':
    main()