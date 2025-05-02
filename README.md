# Distributed Web Crawler

## Recent Updates (v2.0)

This project has been significantly enhanced with the following improvements:

1. **Cloud/Local Hybrid Operation**: The system now gracefully falls back to local file-based operation when Google Cloud services are unavailable or have permission issues.

2. **Improved Resilience**:
   - All components can now operate without Google Cloud dependencies
   - Automatic detection of missing or invalid credentials
   - Seamless transition between cloud and local modes

3. **Flexible Deployment Options**:
   - Run with full Google Cloud integration
   - Run in completely local mode with `--local` flag
   - Docker containers support both modes via environment variables

4. **Enhanced Error Handling**:
   - Proper handling of permission issues
   - Automatic recovery from service disruptions
   - Comprehensive logging for troubleshooting

5. **Simplified Configuration**:
   - Configuration from Cloud Storage, local files, or environment variables
   - Service account auto-detection
   - Sensible defaults for all settings

For running with Google Cloud or in local-only mode, see [`DOCKER_GCP_README.md`](DOCKER_GCP_README.md).

## Original Project Overview

A distributed web crawling and indexing system implemented in Python using MPI for distributed computing. The system efficiently crawls websites while respecting robots.txt directives and builds a searchable index of web pages.

## Features

- **Distributed Crawling**: Multiple crawler nodes work in parallel to fetch web pages
- **Web Page Indexing**: Full-text search indexing of crawled content
- **Scalability**: Easy to add more crawler or indexer nodes
- **Fault Tolerance**: 
  - Automatic recovery from crawler node failures
  - Task reassignment for failed nodes
  - Persistent state storage
- **Politeness**:
  - Respects robots.txt directives
  - Configurable crawl delays
  - Domain-based rate limiting
- **Search Functionality**: Search indexed content by keywords

## System Architecture

- **Master Node**: Coordinates crawling tasks and monitors worker health
- **Crawler Nodes**: Fetch and parse web pages
- **Indexer Nodes**: Build and maintain the search index
- **CLI Interface**: Control and monitor the crawling process

## Requirements

```bash
# Install dependencies
pip install -r requirements.txt
```

The system requires Python 3.8+ and the following main dependencies:
- mpi4py
- requests
- beautifulsoup4
- whoosh
- aiohttp
- robotexclusionrulesparser

## Usage

### Starting a Crawl

```bash
# Start with a single seed URL
python src/cli.py start --url "http://example.com"

# Start with multiple seed URLs from a file
python src/cli.py start --file seeds.txt

# Configure crawl parameters
python src/cli.py start --url "http://example.com" \
    --depth 3 \
    --limit 1000 \
    --delay 1.0 \
    --nodes 5 \
    --allowed-domains "example.com,example.org"
```

### Checking Status

```bash
python src/cli.py status
```

### Searching the Index

```bash
# Basic search
python src/cli.py search "your search query"

# Search with specific field and limit
python src/cli.py search "your search query" --field title --limit 20
```

## Configuration

The system can be configured through command-line arguments or a configuration file:

- `--depth`: Maximum crawl depth (default: 3)
- `--limit`: Maximum pages per domain (default: 1000)
- `--delay`: Default crawl delay in seconds (default: 1.0)
- `--nodes`: Number of nodes to use (default: 3)
- `--allowed-domains`: Comma-separated list of allowed domains
- `--ignore-robots`: Flag to ignore robots.txt directives

## Fault Tolerance

The system implements several fault tolerance mechanisms:

1. **Crawler Node Failure**:
   - Heartbeat monitoring detects failed nodes
   - Tasks from failed nodes are automatically reassigned
   - New nodes can join the cluster dynamically

2. **Indexer Node Failure**:
   - Index data is persisted to disk
   - Multiple indexer nodes can maintain replicated indices
   - Automatic recovery of index state

3. **State Persistence**:
   - Crawl state is periodically saved
   - Can resume from last known state after system restart

## Development

### Project Structure

```
.
├── src/
│   ├── master.py      # Master node implementation
│   ├── crawler.py     # Crawler node implementation
│   ├── indexer.py     # Indexer node implementation
│   └── cli.py         # Command-line interface
├── config/            # Configuration files
├── requirements.txt   # Python dependencies
└── README.md         # This file
```

### Adding New Features

1. **New Crawler Functionality**:
   - Extend the `WebCrawler` class in `crawler.py`
   - Add new message types in the communication protocol

2. **New Indexing Features**:
   - Extend the `SearchIndex` class in `indexer.py`
   - Add new search capabilities or index fields

3. **New CLI Commands**:
   - Add new subparsers in `cli.py`
   - Implement corresponding functionality

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.
