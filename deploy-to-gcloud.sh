#!/bin/bash

# Configuration
PROJECT_ID="distributed-crawler-456413"  # Using the project ID from service account
REGION="us-central1"
ZONE="us-central1-a"
MASTER_INSTANCE="crawler-master"
CRAWLER_INSTANCE="crawler-worker"
INDEXER_INSTANCE="crawler-indexer"
MACHINE_TYPE="e2-medium"
NETWORK_TAG="crawler-network"

# Check if Google Cloud SDK is installed
if ! command -v gcloud &> /dev/null; then
    echo "Error: Google Cloud SDK (gcloud) not found."
    echo "Please install it from: https://cloud.google.com/sdk/docs/install"
    exit 1
fi

# Create GCP resources
echo "Setting up Google Cloud resources..."

# Set the project
gcloud config set project $PROJECT_ID

# Enable required APIs
echo "Enabling required Google Cloud APIs..."
gcloud services enable compute.googleapis.com
gcloud services enable pubsub.googleapis.com
gcloud services enable storage.googleapis.com
gcloud services enable firestore.googleapis.com
gcloud services enable cloudtasks.googleapis.com

# Create Pub/Sub topics
echo "Creating Pub/Sub topics..."
gcloud pubsub topics create web-crawler-crawl-requests || true
gcloud pubsub topics create web-crawler-index-requests || true
gcloud pubsub topics create web-crawler-results || true
gcloud pubsub topics create web-crawler-heartbeats || true

# Create Cloud Storage bucket
echo "Creating Cloud Storage bucket..."
gcloud storage buckets create gs://$PROJECT_ID-crawler-data --location=$REGION || true

# Create Firestore database if it doesn't exist
echo "Setting up Firestore database..."
gcloud firestore databases create --region=$REGION || true

# Create Cloud Tasks queue
echo "Creating Cloud Tasks queue..."
gcloud tasks queues create crawler-tasks --location=$REGION || true

# Build Docker images
echo "Building Docker images..."
docker build -t gcr.io/$PROJECT_ID/master -f Dockerfile.master .
docker build -t gcr.io/$PROJECT_ID/crawler -f Dockerfile.crawler .
docker build -t gcr.io/$PROJECT_ID/indexer -f Dockerfile.indexer .

# Push Docker images to Google Container Registry
echo "Pushing Docker images to Google Container Registry..."
gcloud auth configure-docker
docker push gcr.io/$PROJECT_ID/master
docker push gcr.io/$PROJECT_ID/crawler
docker push gcr.io/$PROJECT_ID/indexer

# Create VPC firewall rule to allow communication between instances
echo "Creating firewall rules..."
gcloud compute firewall-rules create $NETWORK_TAG-allow-internal \
    --direction=INGRESS \
    --priority=1000 \
    --network=default \
    --action=ALLOW \
    --rules=tcp:8000,tcp:22 \
    --source-ranges=10.0.0.0/8 \
    --target-tags=$NETWORK_TAG || true

# Create the master VM instance
echo "Creating master VM instance..."
gcloud compute instances create $MASTER_INSTANCE \
    --zone=$ZONE \
    --machine-type=$MACHINE_TYPE \
    --tags=$NETWORK_TAG \
    --metadata=startup-script='#!/bin/bash
    mkdir -p /app/data
    docker pull gcr.io/'$PROJECT_ID'/master
    docker run -d --name master \
        -p 8000:8000 \
        -v /app/data:/app/data \
        -e GOOGLE_CLOUD_PROJECT='$PROJECT_ID' \
        gcr.io/'$PROJECT_ID'/master
    ' \
    --create-disk=auto-delete=yes,boot=yes,image-family=cos-stable,image-project=cos-cloud || true

# Wait for master to start
echo "Waiting for master to start..."
sleep 30

# Create the crawler VM instance
echo "Creating crawler VM instance..."
gcloud compute instances create $CRAWLER_INSTANCE \
    --zone=$ZONE \
    --machine-type=$MACHINE_TYPE \
    --tags=$NETWORK_TAG \
    --metadata=startup-script='#!/bin/bash
    mkdir -p /app/data
    docker pull gcr.io/'$PROJECT_ID'/crawler
    docker run -d --name crawler \
        -v /app/data:/app/data \
        -e GOOGLE_CLOUD_PROJECT='$PROJECT_ID' \
        -e CRAWLER_MAX_DEPTH=3 \
        -e CRAWLER_MAX_PAGES_PER_DOMAIN=25 \
        -e CRAWLER_RESPECT_ROBOTS=true \
        -e CRAWLER_CRAWL_DELAY=1.0 \
        gcr.io/'$PROJECT_ID'/crawler
    ' \
    --create-disk=auto-delete=yes,boot=yes,image-family=cos-stable,image-project=cos-cloud || true

# Create the indexer VM instance
echo "Creating indexer VM instance..."
gcloud compute instances create $INDEXER_INSTANCE \
    --zone=$ZONE \
    --machine-type=$MACHINE_TYPE \
    --tags=$NETWORK_TAG \
    --metadata=startup-script='#!/bin/bash
    mkdir -p /app/data
    docker pull gcr.io/'$PROJECT_ID'/indexer
    docker run -d --name indexer \
        -v /app/data:/app/data \
        -e GOOGLE_CLOUD_PROJECT='$PROJECT_ID' \
        gcr.io/'$PROJECT_ID'/indexer
    ' \
    --create-disk=auto-delete=yes,boot=yes,image-family=cos-stable,image-project=cos-cloud || true

echo "Deployment complete!"
echo "Master VM external IP: $(gcloud compute instances describe $MASTER_INSTANCE --zone=$ZONE --format='get(networkInterfaces[0].accessConfigs[0].natIP)')"

# Instructions for scaling
echo "To add more crawler workers, run:"
echo "gcloud compute instances create crawler-worker-N \\
    --zone=$ZONE \\
    --machine-type=$MACHINE_TYPE \\
    --tags=$NETWORK_TAG \\
    --metadata=startup-script='#!/bin/bash
    mkdir -p /app/data
    docker pull gcr.io/$PROJECT_ID/crawler
    docker run -d --name crawler \\
        -v /app/data:/app/data \\
        -e GOOGLE_CLOUD_PROJECT=$PROJECT_ID \\
        -e CRAWLER_MAX_DEPTH=3 \\
        -e CRAWLER_MAX_PAGES_PER_DOMAIN=25 \\
        -e CRAWLER_RESPECT_ROBOTS=true \\
        -e CRAWLER_CRAWL_DELAY=1.0 \\
        gcr.io/$PROJECT_ID/crawler
    ' \\
    --create-disk=auto-delete=yes,boot=yes,image-family=cos-stable,image-project=cos-cloud" 