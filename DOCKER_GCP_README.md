# Running the Distributed Web Crawler with Docker and Google Cloud

This project is a distributed web crawler system built to work with Google Cloud services for improved scalability and reliability. The system is designed to gracefully fall back to local execution when cloud services are unavailable or have insufficient permissions.

## System Requirements

- Python 3.8+
- Docker (for containerized deployment)
- Google Cloud SDK (for cloud deployment)
- A service account with permissions for:
  - Cloud Storage
  - Pub/Sub
  - Firestore (optional)

## Running Modes

The system can run in two primary modes:

1. **Cloud Mode**: Uses Google Cloud services for communication and storage
2. **Local Mode**: Uses the local file system for storage and message passing

## Configuration

### Service Account Setup

To use Google Cloud services, you need a service account key:

1. Create a service account in the Google Cloud Console
2. Grant it the necessary permissions:
   - Storage Admin
   - Pub/Sub Admin
   - Firestore Admin (if using Firestore)
3. Download the JSON key file
4. Save it as `config/distributed_crawler.json`

### Configuration File

Create a configuration file at `config/crawl_config.json`:

```json
{
  "seed_urls": ["https://example.com", "https://example.org"],
  "max_depth": 3,
  "max_pages_per_domain": 25,
  "respect_robots": true,
  "crawl_delay": 1.0,
  "allowed_domains": []
}
```

## Running Locally

### Cloud-Connected Mode

To run with Google Cloud services:

```bash
# Ensure your service account key is in the config directory
# Set up environment variable (optional, the system will look for the key in config/ by default)
export GOOGLE_APPLICATION_CREDENTIALS="$(pwd)/config/distributed_crawler.json"

# Run individual components
python src/run_gcloud_crawler.py master
python src/run_gcloud_crawler.py crawler
python src/run_gcloud_crawler.py indexer

# Or run all components together
python src/run_gcloud_crawler.py all
```

### Local-Only Mode

If you don't have Google Cloud access or want to test without cloud services:

```bash
# Run with --local flag to force local mode
python src/run_gcloud_crawler.py master --local
python src/run_gcloud_crawler.py crawler --local
python src/run_gcloud_crawler.py indexer --local

# Or run all components together
python src/run_gcloud_crawler.py all --local
```

In local mode:
- URLs to crawl are passed via files in `data/urls/to_crawl/`
- Results are stored in `data/urls/crawled/`
- HTML content is stored in `data/html/`
- Indexed content is stored in `data/index/`

## Docker Deployment

You can run the system using Docker:

### Build the Images

```bash
docker build -t crawler-master -f Dockerfile.master .
docker build -t crawler-worker -f Dockerfile.crawler .
docker build -t crawler-indexer -f Dockerfile.indexer .
```

### Run in Cloud Mode

```bash
# Create a volume for your config
docker volume create crawler-config

# Copy your service account file to the volume
docker run -v crawler-config:/config --name config-setup ubuntu
docker cp config/distributed_crawler.json config-setup:/config/
docker rm config-setup

# Run the components
docker run -d -v crawler-config:/app/config --name master crawler-master
docker run -d -v crawler-config:/app/config --name crawler crawler-worker
docker run -d -v crawler-config:/app/config --name indexer crawler-indexer
```

### Run in Local Mode

```bash
# Create volumes for data and config
docker volume create crawler-data
docker volume create crawler-config

# Copy your config file if needed
docker run -v crawler-config:/config --name config-setup ubuntu
docker cp config/crawl_config.json config-setup:/config/
docker rm config-setup

# Run components in local mode
docker run -d -v crawler-data:/app/data -v crawler-config:/app/config --name master \
  crawler-master python run_gcloud_crawler.py master --local

docker run -d -v crawler-data:/app/data -v crawler-config:/app/config --name crawler \
  crawler-worker python run_gcloud_crawler.py crawler --local

docker run -d -v crawler-data:/app/data -v crawler-config:/app/config --name indexer \
  crawler-indexer python run_gcloud_crawler.py indexer --local
```

## Using docker-compose

For convenience, a docker-compose.yml file is provided that supports both modes:

```bash
# Run in cloud mode
docker-compose up

# Run in local mode
docker-compose run -e LOCAL_MODE=true
```

## Troubleshooting

### Common Issues

1. **Missing or Invalid Service Account**: If you see "Will use local fallback mode" messages, your service account is missing or invalid. The system will continue in local mode.

2. **Permission Errors**: If creating buckets or pub/sub topics fails, check your service account permissions. The system will use local storage as a fallback.

3. **Local File Access Issues**: Make sure the directories under `data/` are writable.

4. **Cloud Service Quotas**: If you hit quota limits on Google Cloud, the system will fall back to local operation.

### Logs

Check the logs for each component:
- `gcpmaster.log` - Master logs
- `gcpcrawler.log` - Crawler logs
- `gcpindexer.log` - Indexer logs

## Status and Metrics

When running in cloud mode, metrics are available through Google Cloud Monitoring.

In local mode, check the local state files in `data/state/` for progress information.

## Advanced Configuration

For advanced configuration options and additional features, refer to the main README.md file. 