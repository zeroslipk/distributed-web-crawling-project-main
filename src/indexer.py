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
        logging.FileHandler('indexer.log', mode='w')  # clear previous logs
    ]
)

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

class SearchIndex:
    def __init__(self):
        self.content_index = defaultdict(list)  # word -> [(url, score, title, timestamp), ...]
        self.url_index = {}  # url -> {title, text, timestamp}
        
    def tokenize(self, text):
        if not text:
            return []
        return re.findall(r'\w+', text.lower())
    
    def index_document(self, doc):
        url = doc['url']
        title = doc.get('title', '')
        text = doc.get('text', '')
        timestamp = doc.get('timestamp', datetime.utcnow().isoformat())
        
        self.url_index[url] = {
            'title': title,
            'text': text,
            'timestamp': timestamp
        }
        
        for word in self.tokenize(title):
            self.content_index[word].append((url, 2.0, title, timestamp))
        
        for word in self.tokenize(text):
            self.content_index[word].append((url, 1.0, title, timestamp))
    
    def search(self, query, field='content', limit=10):
        query_terms = self.tokenize(query)
        if not query_terms:
            return []
        
        scores = defaultdict(float)
        matched_titles = {}
        matched_times = {}
        
        for term in query_terms:
            if field == 'url':
                for url in self.url_index:
                    if term in url.lower():
                        doc = self.url_index[url]
                        scores[url] += 1.0
                        matched_titles[url] = doc['title']
                        matched_times[url] = doc['timestamp']
            else:
                for url, score, title, timestamp in self.content_index[term]:
                    if field == 'title' and term not in self.tokenize(title):
                        continue
                    scores[url] += score
                    matched_titles[url] = title
                    matched_times[url] = timestamp
        
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

# Create global search index
index = SearchIndex()

def check_search_requests():
    try:
        os.makedirs('data/search_requests', exist_ok=True)
        os.makedirs('data/search_results', exist_ok=True)
        
        for request_file in os.listdir('data/search_requests'):
            if not request_file.endswith('.json'):
                continue
            
            request_path = os.path.join('data/search_requests', request_file)
            try:
                with open(request_path, 'r') as f:
                    request = json.load(f)
                
                results = index.search(
                    query=request['query'],
                    field=request.get('field', 'content'),
                    limit=10
                )
                
                result_file = os.path.join('data/search_results', request_file)
                with open(result_file, 'w') as f:
                    json.dump(results, f)
                    
            except Exception as e:
                logging.error(f"Error processing search request {request_file}: {e}")
            
            try:
                os.remove(request_path)
            except:
                pass
    except Exception as e:
        logging.error(f"Error checking search requests: {e}")

def cleanup_indexer():
    try:
        # Close logging handlers
        for handler in logging.getLogger().handlers[:]:
            handler.close()
            logging.getLogger().removeHandler(handler)
        logging.shutdown()
        
        if os.path.exists('indexer.log'):
            try:
                os.remove('indexer.log')
                logging.info("Deleted indexer.log")
            except Exception as e:
                logging.error(f"Could not delete indexer.log: {e}")
        
        logging.info("Indexer cleanup completed")
    except Exception as e:
        logging.error(f"Error during indexer cleanup: {e}")

def indexer_process():
    logging.info(f"Indexer node started with rank {rank}")

    while True:
        if comm.Iprobe(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG):
            message = comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG)
            
            if isinstance(message, dict) and message.get('command') == 'shutdown':
                logging.info("Received shutdown command from master")
                cleanup_indexer()
                break

            if isinstance(message, dict) and 'url' in message and 'content' in message:
                url = message['url']
                content = message['content']
                
                try:
                    if isinstance(content, dict):
                        index.index_document(content)
                        logging.info(f"Indexed content from URL: {url}")
                    else:
                        logging.warning(f"Received non-dict content for URL: {url}")
                except Exception as e:
                    logging.error(f"Error indexing content from {url}: {str(e)}")
        
        check_search_requests()
        time.sleep(0.1)

if __name__ == '__main__':
    try:
        indexer_process()
    except KeyboardInterrupt:
        logging.info("Received keyboard interrupt, shutting down indexer...")
        cleanup_indexer()
    except Exception as e:
        logging.error(f"Error in indexer process: {e}")
        cleanup_indexer()