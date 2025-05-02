# Distributed Web Crawler with Google Cloud

This project is a distributed web crawler system that uses Google Cloud services for communication, storage, and processing. It replaces the previous MPI-based architecture with cloud services to improve scalability and reliability.

## Components

1. **Master** (`src/gcloud_master.py`): Coordinates the crawling process, distributes URLs to crawlers, and tracks overall state using Google Pub/Sub, Firestore, and Cloud Storage.

2. **Crawler** (`src/gcloud_crawler.py`): Fetches web pages, extracts links and content, and sends results back to the master using Google Pub/Sub and Cloud Storage.

3. **Indexer** (`src/gcloud_indexer.py`): Indexes the content of crawled pages and provides search functionality using Firestore and Cloud Storage.

## Google Cloud Services Used

- **Pub/Sub**: For message passing between components
- **Firestore**: For storing state, search indexes, and document metadata
- **Cloud Storage**: For storing raw HTML content and full-text index snapshots
- **Cloud Tasks**: For reliable URL processing with retries (optional)
- **Compute Engine**: For hosting the components in separate VMs

## Setup

### Prerequisites

1. Google Cloud account with a project
2. Google Cloud SDK installed locally
3. Service account with necessary permissions (Pub/Sub, Firestore, Storage, Compute Engine)
4. Service account key saved as `config/distributed_crawler.json`

### Configuration

The crawler can be configured through environment variables or Firestore:

- `CRAWLER_MAX_DEPTH`: Maximum depth for crawling (default: 3)
- `CRAWLER_MAX_PAGES_PER_DOMAIN`: Maximum pages to crawl per domain (default: 25)
- `CRAWLER_RESPECT_ROBOTS`: Whether to respect robots.txt (default: true)
- `CRAWLER_CRAWL_DELAY`: Delay between requests to the same domain in seconds (default: 1.0)
- `CRAWLER_ALLOWED_DOMAINS`: Comma-separated list of allowed domains (optional)

## Running Locally

You can run the components locally for development and testing:

```bash
# Run all components in separate processes
python src/run_gcloud_crawler.py all

# Or run individual components
python src/run_gcloud_crawler.py master
python src/run_gcloud_crawler.py crawler
python src/run_gcloud_crawler.py indexer
```

## Docker Deployment

The project includes Docker files for containerization:

```bash
# Build and run using docker-compose
docker-compose up --build

# Or build and run individual containers
docker build -t crawler-master -f Dockerfile.master .
docker build -t crawler-worker -f Dockerfile.crawler .
docker build -t crawler-indexer -f Dockerfile.indexer .

docker run -p 8000:8000 -v $(pwd)/config:/app/config crawler-master
docker run -v $(pwd)/config:/app/config crawler-worker
docker run -v $(pwd)/config:/app/config crawler-indexer
```

## Google Cloud Deployment

A deployment script is provided to set up the entire system on Google Cloud:

```bash
# Make the script executable
chmod +x deploy-to-gcloud.sh

# Run the deployment script
./deploy-to-gcloud.sh
```

The script:
1. Creates necessary Google Cloud resources (Pub/Sub topics, Storage bucket, Firestore database)
2. Builds and pushes Docker images to Google Container Registry
3. Sets up Compute Engine VMs to run each component
4. Configures networking between the components

## Scaling

To scale the system:
- Add more crawler instances to process more URLs in parallel
- Add more indexer instances to handle increased indexing load
- The master can be scaled vertically by using a larger VM instance

To add another crawler instance, run:
```bash
gcloud compute instances create crawler-worker-2 \
    --zone=us-central1-a \
    --machine-type=e2-medium \
    --tags=crawler-network \
    --metadata=startup-script='#!/bin/bash
    mkdir -p /app/data
    docker pull gcr.io/YOUR_PROJECT_ID/crawler
    docker run -d --name crawler \
        -v /app/data:/app/data \
        -e GOOGLE_CLOUD_PROJECT=YOUR_PROJECT_ID \
        -e CRAWLER_MAX_DEPTH=3 \
        -e CRAWLER_MAX_PAGES_PER_DOMAIN=25 \
        -e CRAWLER_RESPECT_ROBOTS=true \
        -e CRAWLER_CRAWL_DELAY=1.0 \
        gcr.io/YOUR_PROJECT_ID/crawler
    ' \
    --create-disk=auto-delete=yes,boot=yes,image-family=cos-stable,image-project=cos-cloud
```

## Monitoring

The system reports metrics and logs to Google Cloud:
- Component status is reported via heartbeats through Pub/Sub
- Logs are stored in standard output, which can be viewed in Google Cloud Logging
- Documents indexed are tracked in Firestore

## Troubleshooting

If you encounter issues:
1. Check the VM logs: `gcloud compute instances get-serial-port-output INSTANCE_NAME --zone=ZONE`
2. SSH into the VMs: `gcloud compute ssh INSTANCE_NAME --zone=ZONE`
3. Check Docker logs: `docker logs container_name`
4. Check Google Cloud services are properly enabled and permissions are set correctly

## Data Access

- Raw HTML content: Stored in Cloud Storage bucket under the `html/` prefix
- Indexed documents: Available in Firestore's `indexed_docs` collection
- Search index: Stored in both Cloud Storage (for snapshots) and Firestore (for active queries) 