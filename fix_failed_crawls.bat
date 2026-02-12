@echo off
echo ========================================
echo Fixing crawler with failed URLs
echo ========================================

rem Stop any running containers
echo Stopping existing containers...
docker-compose down

rem Clear the problematic domains flag
echo Creating clear_problematic_domains flag...
echo Clear request from fix script at %date% %time% > clear_problematic_domains.flag

rem Update the configuration
echo Updating crawler configuration...
echo {> config/crawl_config.json
echo   "seed_urls": [>> config/crawl_config.json
echo     "https://en.wikipedia.org/wiki/Main_Page">> config/crawl_config.json
echo   ],>> config/crawl_config.json
echo   "max_depth": 2,>> config/crawl_config.json
echo   "max_pages_per_domain": 12,>> config/crawl_config.json
echo   "respect_robots": true,>> config/crawl_config.json
echo   "crawl_delay": 1.0,>> config/crawl_config.json
echo   "allowed_domains": [>> config/crawl_config.json
echo     "en.wikipedia.org">> config/crawl_config.json
echo   ],>> config/crawl_config.json
echo   "timeout": 30,>> config/crawl_config.json
echo   "max_retries": 3,>> config/crawl_config.json
echo   "start_time": "%date% %time%">> config/crawl_config.json
echo }>> config/crawl_config.json

rem Start containers
echo Starting containers with updated configuration...
docker-compose up -d

echo ========================================
echo Fix completed. Monitoring logs...
echo ========================================
echo Press Ctrl+C to exit log view

rem Monitor the logs to see if the issue is resolved
docker-compose logs -f master

rem When user exits logs with Ctrl+C, show status
echo ========================================
echo Current status:
echo ========================================
docker-compose ps 