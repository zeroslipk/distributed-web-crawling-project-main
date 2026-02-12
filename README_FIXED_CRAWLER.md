# Distributed Web Crawler - Fix for Failed URLs

This document explains the changes made to address the issue where the crawler was reporting 18 failed URLs and getting stuck in a retry loop.

## Problem Identified

The crawler was experiencing the following issues:

1. URLs were continuously timing out and being re-queued without any limit
2. No detailed error reporting about why URLs were failing
3. The timeout settings were too long (60 seconds)
4. The system lacked a mechanism to permanently mark problematic domains

## Solutions Implemented

### 1. Fixed URL Timeout Handling

- Added a retry counter to limit the number of times a URL can be re-queued
- Added detailed logging for timeout reasons
- Improved reporting of failed URLs in the statistics output

```python
# In check_timeouts function
if retry_count > max_retries:
    self.state.failed.add(url)
    logging.warning(f"URL timed out and permanently failed after {retry_count} retries: {url}")
```

### 2. Enhanced Error Reporting

- Added error_detail field to capture more specific error information
- Updated the failure_reasons tracking to maintain a list of why URLs failed
- Modified the stats output to show failure reasons

```python
failure_summary = ", ".join([f"{count} {reason}" for reason, count in failure_counts.items()])
logging.info(f"Stats: Queue={len(self.state.url_queue)}, In Progress={len(self.state.in_progress)}, Completed={len(self.state.completed)}, Failed={len(self.state.failed)}/{failure_summary}")
```

### 3. Configuration Improvements

- Added timeout and max_retries parameters to the configuration
- Updated docker-compose.yml with reasonable timeout settings
- Created a fix script to reset the crawler with better settings

### 4. Added Reset Functionality

- Added CLI commands to reset failed URLs
- Added a mechanism to clear problematic domains
- Created Windows and Linux scripts to perform all reset operations

## How to Fix Stuck Crawlers

If you see the crawler stuck with a large number of failed URLs:

1. Run the fix script:
   - On Linux/Mac: `./fix_failed_crawls.sh`  
   - On Windows: `fix_failed_crawls.bat`

2. Or use the CLI:
   ```
   python -m src.cli reset-failures --clear-problematic-domains
   python -m src.cli reset-failures --reset-failed-urls
   ```

3. Monitor the logs to ensure the crawler is making progress:
   ```
   docker-compose logs -f master
   ```

## Preventing Future Issues

1. Keep reasonable timeout values (30 seconds is recommended)
2. Limit retry attempts to 3 
3. Periodically check logs for problematic patterns
4. Consider adding more allowed_domains to restrict crawling to specific sites

These changes should prevent the crawler from getting stuck in endless timeout loops and provide better visibility into why URLs are failing. 