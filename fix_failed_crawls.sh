#!/bin/bash

echo "========================================"
echo "Fixing crawler with failed URLs"
echo "========================================"

# Stop any running containers
echo "Stopping existing containers..."
docker-compose down

# Clear the problematic domains flag
echo "Creating clear_problematic_domains flag..."
echo "Clear request from fix script at $(date)" > clear_problematic_domains.flag

# Update the configuration
echo "Updating crawler configuration..."
cat > config/crawl_config.json << EOL
{
  "seed_urls": [
    "https://en.wikipedia.org/wiki/Main_Page"
  ],
  "max_depth": 2,
  "max_pages_per_domain": 12,
  "respect_robots": true,
  "crawl_delay": 1.0,
  "allowed_domains": [
    "en.wikipedia.org"
  ],
  "timeout": 30,
  "max_retries": 3,
  "start_time": "$(date -Iseconds)"
}
EOL

# Start containers
echo "Starting containers with updated configuration..."
docker-compose up -d

echo "========================================"
echo "Fix completed. Monitoring logs..."
echo "========================================"
echo "Press Ctrl+C to exit log view"

# Monitor the logs to see if the issue is resolved
docker-compose logs -f master

# When user exits logs with Ctrl+C, show status
echo "========================================"
echo "Current status:"
echo "========================================"
docker-compose ps 