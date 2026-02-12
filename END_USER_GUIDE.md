# Distributed Web Crawler - End User Guide

## Table of Contents
1. [Introduction](#introduction)
2. [Getting Started](#getting-started)
3. [Running the System](#running-the-system)
4. [Searching Content](#searching-content)
5. [Monitoring and Management](#monitoring-and-management)
6. [Troubleshooting](#troubleshooting)

## Introduction

Welcome to the Distributed Web Crawler! This system allows you to efficiently crawl and index web content across multiple machines. The system is designed to be scalable, fault-tolerant, and respectful of website crawling policies.

### Key Features
- Distributed web crawling across multiple nodes
- Full-text search indexing
- Automatic fault recovery
- Respect for robots.txt and crawl delays
- Flexible deployment options (local or cloud)

## Getting Started

### Prerequisites
- Python 3.8 or higher
- Docker (for containerized deployment)
- Google Cloud account (optional, for cloud deployment)

### Installation

1. Clone the repository:
```bash
git clone [repository-url]
cd distributed-web-crawling-project
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Running the System

### Local Mode

1. Start a basic crawl with a single URL:
```bash
python src/cli.py start --url "http://example.com"
```

2. Start with multiple seed URLs:
```bash
python src/cli.py start --file seeds.txt
```

3. Configure advanced crawl parameters:
```bash
python src/cli.py start --url "http://example.com" \
    --depth 3 \
    --limit 1000 \
    --delay 1.0 \
    --nodes 5 \
    --allowed-domains "example.com,example.org"
```

### Docker Mode

1. Build and start all services:
```bash
docker-compose up --build
```

2. Run in local-only mode:
```bash
docker-compose -f docker-compose.yml up --build --env-file .env.local
```

### Cloud Mode (Google Cloud)

1. Set up Google Cloud credentials:
```bash
gcloud auth login
gcloud config set project [YOUR_PROJECT_ID]
```

2. Deploy to Google Cloud:
```bash
./deploy-to-gcloud.sh
```

## Searching Content

### Basic Search
```bash
python src/cli.py search "your search query"
```

### Advanced Search
```bash
# Search in specific field with result limit
python src/cli.py search "your search query" --field title --limit 20
```

## Monitoring and Management

### Check System Status
```bash
python src/cli.py status
```

### View Logs
- Master node logs: `master.log`
- Crawler node logs: `crawler.log`
- Cloud deployment logs: `gcpmaster.log`, `gcpcrawler.log`, `gcpindexer.log`

## Troubleshooting

### Common Issues

1. **Crawler Not Starting**
   - Check if all dependencies are installed
   - Verify network connectivity
   - Check log files for specific errors

2. **Search Not Working**
   - Ensure indexer service is running
   - Check if content has been crawled and indexed
   - Verify search query syntax

3. **Cloud Deployment Issues**
   - Verify Google Cloud credentials
   - Check service account permissions
   - Review cloud deployment logs

### Getting Help

If you encounter issues:
1. Check the log files in the project directory
2. Review the configuration settings
3. Ensure all services are running properly
4. Check network connectivity and firewall settings

## Best Practices

1. **Crawl Configuration**
   - Start with a small depth and limit for testing
   - Use appropriate crawl delays to be respectful to websites
   - Specify allowed domains to focus crawling

2. **Resource Management**
   - Monitor system resources during crawling
   - Adjust number of nodes based on available resources
   - Use appropriate limits for your use case

3. **Search Optimization**
   - Use specific fields for more targeted searches
   - Limit results when dealing with large datasets
   - Use appropriate search terms for better results

## Support

For additional support:
- Check the project documentation
- Review the README files
- Check the logs for detailed error information
- Contact the development team for specific issues 