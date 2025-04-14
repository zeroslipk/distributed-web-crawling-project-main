from mpi4py import MPI
import logging
import json
from collections import defaultdict
import re
from datetime import datetime
import os
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - Indexer - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('indexer.log')
    ]
)

class SearchIndex:
    def __init__(self):
        self.content_index = defaultdict(list)  # word -> [(url, score, title, timestamp), ...]
        self.url_index = {}  # url -> {title, text, timestamp}
        
    def tokenize(self, text):
        """Convert text to lowercase and split into words"""
        if not text:
            return []
        # Convert to lowercase and split on non-alphanumeric characters
        return re.findall(r'\w+', text.lower())
    
    def index_document(self, doc):
        """Index a document by its content and title"""
        url = doc['url']
        title = doc.get('title', '')
        text = doc.get('text', '')
        timestamp = doc.get('timestamp', datetime.utcnow().isoformat())
        
        # Store the full document
        self.url_index[url] = {
            'title': title,
            'text': text,
            'timestamp': timestamp
        }
        
        # Index title words (higher weight)
        for word in self.tokenize(title):
            self.content_index[word].append((url, 2.0, title, timestamp))
        
        # Index content words
        for word in self.tokenize(text):
            self.content_index[word].append((url, 1.0, title, timestamp))
    
    def search(self, query, field='content', limit=10):
        """Search the index"""
        query_terms = self.tokenize(query)
        if not query_terms:
            return []
        
        # Score documents based on term frequency and field weights
        scores = defaultdict(float)
        matched_titles = {}  # Keep track of titles for matched URLs
        matched_times = {}   # Keep track of timestamps for matched URLs
        
        for term in query_terms:
            if field == 'url':
                # Direct URL match
                for url in self.url_index:
                    if term in url.lower():
                        doc = self.url_index[url]
                        scores[url] += 1.0
                        matched_titles[url] = doc['title']
                        matched_times[url] = doc['timestamp']
            else:
                # Content or title match
                for url, score, title, timestamp in self.content_index[term]:
                    if field == 'title' and term not in self.tokenize(title):
                        continue
                    scores[url] += score
                    matched_titles[url] = title
                    matched_times[url] = timestamp
        
        # Sort results by score
        results = [
            {
                'url': url,
                'score': score,
                'title': matched_titles[url],
                'timestamp': matched_times[url]
            }
            for url, score in scores.items()
        ]
        results.sort(key=lambda x: x['score'], reverse=True)
        
        return results[:limit]

def check_search_requests():
    """Check for new search requests and process them"""
    try:
        os.makedirs('data/search_requests', exist_ok=True)
        os.makedirs('data/search_results', exist_ok=True)
        
        # Look for search request files
        for request_file in os.listdir('data/search_requests'):
            if not request_file.endswith('.json'):
                continue
                
            request_path = os.path.join('data/search_requests', request_file)
            try:
                with open(request_path, 'r') as f:
                    request = json.load(f)
                
                # Process search request
                results = index.search(
                    query=request['query'],
                    field=request.get('field', 'content'),
                    limit=10
                )
                
                # Save results
                result_file = os.path.join('data/search_results', request_file)
                with open(result_file, 'w') as f:
                    json.dump(results, f)
                    
            except Exception as e:
                logging.error(f"Error processing search request {request_file}: {e}")
            
            try:
                os.remove(request_path)  # Clean up request file
            except:
                pass
                
    except Exception as e:
        logging.error(f"Error checking search requests: {e}")

def indexer_process():
    """Process for an indexer node"""
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    
    logging.info(f"Indexer node started with rank {rank}")
    
    global index
    index = SearchIndex()
    
    try:
        while True:
            # Check for MPI messages
            status = MPI.Status()
            if comm.Iprobe(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status):
                tag = status.Get_tag()
                source = status.Get_source()
                
                if tag == 2:  # Index content
                    content = comm.recv(source=source, tag=2)
                    if content:
                        index.index_document(content)
                        logging.info(f"Indexed content from {content['url']}")
                
                elif tag == 0:  # Shutdown signal
                    logging.info("Received shutdown signal")
                    break
            
            # Check for file-based search requests
            check_search_requests()
            
            # Small delay to prevent busy waiting
            time.sleep(0.1)
            
    except Exception as e:
        logging.error(f"Error in indexer process: {e}")

if __name__ == '__main__':
    indexer_process()