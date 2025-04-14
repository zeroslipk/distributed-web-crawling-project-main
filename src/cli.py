#!/usr/bin/env python3
import argparse
import json
import os
import sys
from mpi4py import MPI
import logging
import time
from urllib.parse import urlparse

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - CLI - %(levelname)s - %(message)s'
)

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
        logging.error("No valid seed URLs provided")
        return

    # Save configuration
    config = {
        'seed_urls': seed_urls,
        'max_depth': args.depth,
        'max_pages_per_domain': args.limit,
        'respect_robots': not args.ignore_robots,
        'crawl_delay': args.delay,
        'allowed_domains': args.allowed_domains.split(',') if args.allowed_domains else None,
        'start_time': time.strftime('%Y-%m-%d %H:%M:%S')
    }

    os.makedirs('config', exist_ok=True)
    with open('config/crawl_config.json', 'w') as f:
        json.dump(config, f, indent=2)

    # Start the crawl using mpiexec
    num_nodes = args.nodes if args.nodes else 3  # Default to 3 nodes (1 master, 1 crawler, 1 indexer)
    cmd = f"mpiexec -n {num_nodes} python src/master.py"
    logging.info(f"Starting crawl with command: {cmd}")
    os.system(cmd)

def show_status(args):
    """Show current crawl status"""
    try:
        with open('crawl_state.json', 'r') as f:
            state = json.load(f)
        
        print("\nCrawl Status:")
        print("-" * 50)
        print(f"Queue Size: {len(state['queue'])}")
        print(f"Completed URLs: {len(state['completed'])}")
        print(f"Failed URLs: {len(state['failed'])}")
        print(f"Total Unique URLs Seen: {len(state['seen_urls'])}")
        print(f"Domains Crawled: {len(state['domain_counts'])}")
        print(f"Last Updated: {state['timestamp']}")
        print("-" * 50)
    except FileNotFoundError:
        logging.error("No crawl state found. Has a crawl been started?")
    except Exception as e:
        logging.error(f"Error reading crawl state: {e}")

def search_index(args):
    """Search the index for the given query"""
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    if size < 2:
        logging.error("Need at least one indexer node to search")
        return

    # Send search query to first indexer node
    indexer_rank = size - 1  # Last node is indexer
    query_msg = {
        'query': args.query,
        'field': args.field,
        'limit': args.limit
    }
    
    try:
        comm.send(query_msg, dest=indexer_rank, tag=3)  # Tag 3 for search queries
        results = comm.recv(source=indexer_rank, tag=4)  # Tag 4 for search results
        
        print("\nSearch Results:")
        print("-" * 50)
        for i, result in enumerate(results, 1):
            print(f"\n{i}. {result['title']}")
            print(f"URL: {result['url']}")
            print(f"Score: {result['score']:.2f}")
            print(f"Indexed: {result['timestamp']}")
    except Exception as e:
        logging.error(f"Error performing search: {e}")

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
    start_parser.add_argument('--allowed-domains', help='Comma-separated list of allowed domains')
    start_parser.add_argument('--ignore-robots', action='store_true', help='Ignore robots.txt')

    # Status command
    status_parser = subparsers.add_parser('status', help='Show crawl status')

    # Search command
    search_parser = subparsers.add_parser('search', help='Search the index')
    search_parser.add_argument('query', help='Search query')
    search_parser.add_argument('--field', default='content', choices=['content', 'title', 'url'],
                             help='Field to search in')
    search_parser.add_argument('--limit', type=int, default=10, help='Maximum number of results')

    args = parser.parse_args()

    if args.command == 'start':
        start_crawl(args)
    elif args.command == 'status':
        show_status(args)
    elif args.command == 'search':
        search_index(args)
    else:
        parser.print_help()

if __name__ == '__main__':
    main() 