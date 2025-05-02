import os
import sys
import logging
import json
import time
import threading
import signal
import hashlib
from datetime import datetime
from collections import defaultdict
import re
import uuid

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - GCP Indexer - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('gcpindexer.log', mode='w')
    ]
)

# Late imports for Google Cloud services to allow local mode without GCP dependencies
google_cloud_available = True
try:
    from google.cloud import pubsub_v1, storage, firestore
except ImportError:
    google_cloud_available = False
    logging.warning("Google Cloud libraries not available. Will run in local-only mode.")

class SearchIndex:
    def __init__(self):
        self.content_index = defaultdict(list)
        self.url_index = {}
        
    def tokenize(self, text):
        """Tokenize text into words"""
        if not text:
            return []
        return re.findall(r'\w+', text.lower())
    
    def index_document(self, doc):
        """Index a document's content"""
        url = doc['url']
        title = doc.get('title', '')
        text = doc.get('text', '')
        timestamp = doc.get('timestamp', datetime.utcnow().isoformat())
        
        self.url_index[url] = {
            'title': title,
            'text': text,
            'timestamp': timestamp
        }
        
        # Index words in title with higher weight
        for word in self.tokenize(title):
            self.content_index[word].append((url, 2.0, title, timestamp))
        
        # Index words in text content
        for word in self.tokenize(text):
            self.content_index[word].append((url, 1.0, title, timestamp))
    
    def search(self, query, field='content', limit=10):
        """Search the index for results matching the query"""
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

class GCPIndexer:
    def __init__(self, project_id=None, topic_name_prefix='web-crawler', local_mode=False):
        self.project_id = project_id
        self.worker_id = str(uuid.uuid4())[:8]  # Generate a unique worker ID
        self.local_mode = local_mode or not google_cloud_available
        
        # Initialize GCP clients based on mode
        self.gcp_available = False
        if not self.local_mode and google_cloud_available:
            try:
                self.publisher = pubsub_v1.PublisherClient()
                self.storage_client = storage.Client()
                self.firestore_client = firestore.Client()
                self.gcp_available = True
                
                # Topic and subscription names
                self.indexer_topic_name = f"projects/{project_id}/topics/{topic_name_prefix}-index-requests"
                self.heartbeat_topic_name = f"projects/{project_id}/topics/{topic_name_prefix}-heartbeats"
                
                # Create subscription
                self.subscription_name = f"projects/{project_id}/subscriptions/{topic_name_prefix}-indexer-{self.worker_id}"
                self._create_subscription()
                
                # Cloud Storage bucket for storing search index snapshots
                self.bucket_name = f"{project_id}-crawler-data"
                self._ensure_bucket_exists()
                
                # Firestore collection for storing index data
                self.docs_collection = self.firestore_client.collection('indexed_docs')
                self.index_collection = self.firestore_client.collection('index_entries')
                self.search_requests_collection = self.firestore_client.collection('search_requests')
                self.search_results_collection = self.firestore_client.collection('search_results')
                
                # Subscriber client for pulling messages
                self.subscriber = pubsub_v1.SubscriberClient()
                
                logging.info("GCP Indexer initialized with Google Cloud services")
            except Exception as e:
                logging.error(f"Failed to initialize Google Cloud services: {e}")
                self.local_mode = True
                self.gcp_available = False
        else:
            logging.info("Running in local-only mode")
            self.gcp_available = False
        
        # Create local directories for storage in all modes (fallback in cloud mode)
        os.makedirs('data/html', exist_ok=True)
        os.makedirs('data/index', exist_ok=True)
        os.makedirs('data/index/history', exist_ok=True)
        os.makedirs('data/urls/to_index', exist_ok=True)
        os.makedirs('data/urls/indexed', exist_ok=True)
        
        # Create a local search index for faster operations
        self.index = SearchIndex()
        
        # Load existing index if available
        self._load_index_from_storage()
                
        # Flag for controlling the indexer
        self.running = True
        
        # Last snapshot time
        self.last_snapshot_time = time.time()
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Handle termination signals"""
        logging.info(f"Received signal {signum}, shutting down...")
        self.running = False
        # Save the index before shutting down
        self._save_index_to_storage()

    def _create_subscription(self):
        """Create a Pub/Sub subscription for this indexer instance"""
        try:
            subscriber = pubsub_v1.SubscriberClient()
            try:
                subscriber.get_subscription(request={"subscription": self.subscription_name})
                logging.info(f"Subscription {self.subscription_name} already exists")
            except Exception:
                try:
                    subscription = subscriber.create_subscription(
                        request={
                            "name": self.subscription_name,
                            "topic": self.indexer_topic_name,
                            "ack_deadline_seconds": 60,
                            "message_retention_duration": {"seconds": 3600}  # 1 hour
                        }
                    )
                    logging.info(f"Created subscription: {subscription.name}")
                except Exception as e:
                    logging.error(f"Failed to create subscription: {e}")
                    logging.info("Will continue with local file functionality.")
            finally:
                subscriber.close()
        except Exception as e:
            logging.error(f"Error with subscription client: {e}")

    def _ensure_bucket_exists(self):
        """Make sure the Storage bucket exists"""
        try:
            self.storage_client.get_bucket(self.bucket_name)
            logging.info(f"Bucket {self.bucket_name} exists")
        except Exception:
            try:
                bucket = self.storage_client.create_bucket(self.bucket_name)
                logging.info(f"Created new bucket: {bucket.name}")
            except Exception as e:
                logging.error(f"Failed to create or access bucket: {e}")
                logging.info("Will use local storage for index content")

    def _load_index_from_storage(self):
        """Load the search index from Cloud Storage or local storage"""
        if not self.local_mode and self.gcp_available:
            try:
                bucket = self.storage_client.bucket(self.bucket_name)
                blob = bucket.blob('index/latest_index.json')
                
                if blob.exists():
                    # Download the index file
                    index_data = json.loads(blob.download_as_string())
                    
                    # Reconstruct the index
                    self.index.url_index = index_data.get('url_index', {})
                    
                    # Reconstruct content index
                    content_index_data = index_data.get('content_index', {})
                    for word, entries in content_index_data.items():
                        self.index.content_index[word] = [
                            (entry[0], entry[1], entry[2], entry[3])
                            for entry in entries
                        ]
                    
                    logging.info(f"Loaded index from Cloud Storage with {len(self.index.url_index)} documents")
                    return True
            except Exception as e:
                logging.error(f"Error loading index from Cloud Storage: {e}")
                # Continue to try local file
        
        # Try loading from local file
        try:
            local_index_path = 'data/index/latest_index.json'
            if os.path.exists(local_index_path):
                with open(local_index_path, 'r') as f:
                    index_data = json.load(f)
                
                # Reconstruct the index
                self.index.url_index = index_data.get('url_index', {})
                
                # Reconstruct content index
                content_index_data = index_data.get('content_index', {})
                for word, entries in content_index_data.items():
                    self.index.content_index[word] = [
                        (entry[0], entry[1], entry[2], entry[3])
                        for entry in entries
                    ]
                
                logging.info(f"Loaded index from local file with {len(self.index.url_index)} documents")
                return True
        except Exception as e:
            logging.error(f"Error loading index from local file: {e}")
        
        logging.info("No index found, starting with empty index")
        return False

    def _save_index_to_storage(self):
        """Save the search index to Cloud Storage or local file"""
        # Convert index to serializable format
        index_data = {
            'url_index': self.index.url_index,
            'content_index': {
                word: [(url, score, title, timestamp) for url, score, title, timestamp in entries]
                for word, entries in self.index.content_index.items()
            },
            'timestamp': datetime.utcnow().isoformat(),
            'indexed_document_count': len(self.index.url_index)
        }
        
        if not self.local_mode and self.gcp_available:
            try:
                # Upload to Cloud Storage
                bucket = self.storage_client.bucket(self.bucket_name)
                
                # Save current index as latest
                latest_blob = bucket.blob('index/latest_index.json')
                latest_blob.upload_from_string(
                    json.dumps(index_data),
                    content_type='application/json'
                )
                
                # Also save a timestamped version for history
                timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
                history_blob = bucket.blob(f'index/history/index_{timestamp}.json')
                history_blob.upload_from_string(
                    json.dumps(index_data),
                    content_type='application/json'
                )
                
                self.last_snapshot_time = time.time()
                logging.info(f"Saved index to Cloud Storage with {len(self.index.url_index)} documents")
                return True
            except Exception as e:
                logging.error(f"Error saving index to Cloud Storage: {e}")
                # Fall back to local storage
        
        # Save to local storage
        try:
            # Save current index as latest
            with open('data/index/latest_index.json', 'w') as f:
                json.dump(index_data, f)
            
            # Also save a timestamped version for history
            timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
            with open(f'data/index/history/index_{timestamp}.json', 'w') as f:
                json.dump(index_data, f)
            
            self.last_snapshot_time = time.time()
            logging.info(f"Saved index to local storage with {len(self.index.url_index)} documents")
            return True
        except Exception as e:
            logging.error(f"Error saving index to local storage: {e}")
            return False

    def save_to_firestore(self, doc):
        """Save indexed document to Firestore or local file"""
        url = doc['url']
        
        if not self.local_mode and self.gcp_available:
            try:
                # Save document info
                doc_ref = self.docs_collection.document(hashlib.md5(url.encode()).hexdigest())
                doc_ref.set({
                    'url': url,
                    'title': doc.get('title', ''),
                    'snippet': doc.get('text', '')[:200] + '...' if doc.get('text') else '',
                    'timestamp': doc.get('timestamp'),
                    'indexed_at': datetime.utcnow()
                })
                
                # Save word entries
                batch = self.firestore_client.batch()
                batch_count = 0
                batch_limit = 500  # Firestore batch write limit
                
                # Index terms in title and text
                for field, weight in [('title', 2.0), ('text', 1.0)]:
                    if field in doc and doc[field]:
                        terms = self.index.tokenize(doc[field])
                        for term in terms:
                            # Create a unique ID for each term-document pair
                            entry_id = f"{term}_{hashlib.md5(url.encode()).hexdigest()}"
                            entry_ref = self.index_collection.document(entry_id)
                            
                            batch.set(entry_ref, {
                                'term': term,
                                'url': url,
                                'title': doc.get('title', ''),
                                'weight': weight,
                                'field': field,
                                'timestamp': datetime.utcnow()
                            })
                            
                            batch_count += 1
                            if batch_count >= batch_limit:
                                batch.commit()
                                batch = self.firestore_client.batch()
                                batch_count = 0
                
                # Commit any remaining entries
                if batch_count > 0:
                    batch.commit()
                
                logging.info(f"Saved document to Firestore: {url}")
                return True
            except Exception as e:
                logging.error(f"Error saving to Firestore: {e}")
                # Fall back to local storage
        
        # Save metadata to local file
        try:
            os.makedirs('data/index/docs', exist_ok=True)
            doc_id = hashlib.md5(url.encode()).hexdigest()
            with open(f'data/index/docs/{doc_id}.json', 'w') as f:
                json.dump({
                    'url': url,
                    'title': doc.get('title', ''),
                    'snippet': doc.get('text', '')[:200] + '...' if doc.get('text') else '',
                    'timestamp': doc.get('timestamp'),
                    'indexed_at': datetime.utcnow().isoformat()
                }, f)
            
            logging.debug(f"Saved document metadata locally: {url}")
            return True
        except Exception as e:
            logging.error(f"Error saving document metadata locally: {e}")
            return False

    def check_local_index_files(self):
        """Check for files to index in the local file system"""
        index_dir = 'data/urls/to_index'
        if not os.path.exists(index_dir):
            return None
            
        files = [f for f in os.listdir(index_dir) if f.endswith('.json')]
        if not files:
            return None
            
        # Process one file
        filepath = os.path.join(index_dir, files[0])
        try:
            with open(filepath, 'r') as f:
                data = json.load(f)
            
            # Remove the file after reading
            os.rename(filepath, f"data/urls/indexed/{os.path.basename(filepath)}")
            
            return data
        except Exception as e:
            logging.error(f"Error processing local index file {filepath}: {e}")
            # Try to move the file to a failed directory
            try:
                os.makedirs('data/urls/failed', exist_ok=True)
                os.rename(filepath, f"data/urls/failed/{os.path.basename(filepath)}")
            except:
                pass
            return None

    def process_message(self, message_data):
        """Process an indexing message from Pub/Sub or local file"""
        try:
            if isinstance(message_data, dict):
                data = message_data
            elif isinstance(message_data, str):
                data = json.loads(message_data)
            else:
                data = json.loads(message_data.data.decode('utf-8'))
            
            url = data.get('url')
            content = data.get('content')
            
            if not url or not content:
                logging.warning("Received message without URL or content")
                if hasattr(message_data, 'ack'):
                    message_data.ack()
                return
            
            logging.info(f"Indexing content for URL: {url}")
            
            # Index the document
            self.index.index_document(content)
            
            # Also save to Firestore for redundancy and queryability
            self.save_to_firestore(content)
            
            # Acknowledge the message if from Pub/Sub
            if hasattr(message_data, 'ack'):
                message_data.ack()
            
            # Periodically snapshot the index (every 50 documents or 10 minutes)
            doc_count = len(self.index.url_index)
            if doc_count % 50 == 0 or time.time() - self.last_snapshot_time > 600:
                self._save_index_to_storage()
            
            logging.info(f"Completed indexing URL: {url}, total indexed: {doc_count}")
        except Exception as e:
            logging.error(f"Error processing index message: {e}")
            if hasattr(message_data, 'nack'):
                message_data.nack()

    def run(self):
        """Main indexing process"""
        self.running = True
        
        # Start heartbeat thread if using GCP
        if not self.local_mode and self.gcp_available:
            heartbeat_thread = threading.Thread(target=self._heartbeat_loop)
            heartbeat_thread.daemon = True
            heartbeat_thread.start()
            
            # Also start search request handling in GCP mode
            search_thread = threading.Thread(target=self._search_loop)
            search_thread.daemon = True
            search_thread.start()
        
        logging.info(f"Indexer {self.worker_id} is running in {'GCP' if self.gcp_available else 'local'} mode...")
        
        if not self.local_mode and self.gcp_available:
            # Set up the subscriber for cloud mode
            try:
                flow_control = pubsub_v1.types.FlowControl(max_messages=5)
                
                # Define the callback function for received messages
                def callback(message):
                    if not self.running:
                        message.nack()
                        return
                    
                    self.process_message(message)
                
                # Start the subscriber
                future = self.subscriber.subscribe(
                    self.subscription_name, 
                    callback,
                    flow_control=flow_control
                )
                
                # Wait for the future to complete
                future.result()
            except Exception as e:
                logging.error(f"Pub/Sub error: {e}")
                logging.info("Falling back to local file mode")
                # Continue in local file mode below
        
        # Local file processing loop (used as fallback in cloud mode)
        try:
            while self.running:
                # Check for files to index
                message_data = self.check_local_index_files()
                if message_data:
                    self.process_message(message_data)
                
                # Sleep to avoid tight loop
                time.sleep(1)
                
                # Periodically save the index
                if time.time() - self.last_snapshot_time > 300:  # Every 5 minutes
                    self._save_index_to_storage()
        except KeyboardInterrupt:
            logging.info("Keyboard interrupt received, shutting down...")
        except Exception as e:
            logging.error(f"Error in indexer loop: {e}")
        finally:
            # Save the index before exiting
            self._save_index_to_storage()
            if self.gcp_available:
                try:
                    self.subscriber.close()
                except:
                    pass
            logging.info("Indexer shutdown complete")

    def _heartbeat_loop(self):
        """Send heartbeats periodically"""
        while self.running:
            self.send_heartbeat()
            time.sleep(30)  # Send heartbeat every 30 seconds

    def _search_loop(self):
        """Handle search requests periodically"""
        while self.running:
            self.handle_search_requests()
            time.sleep(5)  # Check for search requests every 5 seconds

    def send_heartbeat(self):
        """Send heartbeat to indicate the indexer is alive"""
        try:
            heartbeat = {
                'worker_id': self.worker_id,
                'worker_type': 'indexer',
                'timestamp': datetime.utcnow().isoformat(),
                'status': 'active',
                'indexed_document_count': len(self.index.url_index),
                'term_count': len(self.index.content_index)
            }
            
            # Publish heartbeat
            future = self.publisher.publish(
                self.heartbeat_topic_name, 
                data=json.dumps(heartbeat).encode('utf-8')
            )
            message_id = future.result()
            
            logging.debug(f"Sent heartbeat, message ID: {message_id}")
            return True
        except Exception as e:
            logging.error(f"Error sending heartbeat: {e}")
            return False

    def handle_search_requests(self):
        """Check for and process search requests from Firestore"""
        try:
            # Query for new search requests
            search_requests = self.search_requests_collection.where('status', '==', 'pending').limit(10).stream()
            
            for request_doc in search_requests:
                request_id = request_doc.id
                request_data = request_doc.to_dict()
                
                query = request_data.get('query', '')
                field = request_data.get('field', 'content')
                limit = request_data.get('limit', 10)
                
                if not query:
                    # Mark as error if no query
                    self.search_requests_collection.document(request_id).update({
                        'status': 'error',
                        'error': 'No query provided',
                        'processed_at': datetime.utcnow()
                    })
                    continue
                
                # Perform the search
                results = self.index.search(query, field, limit)
                
                # Store the results
                self.search_results_collection.document(request_id).set({
                    'request_id': request_id,
                    'query': query,
                    'results': results,
                    'result_count': len(results),
                    'timestamp': datetime.utcnow()
                })
                
                # Update the request status
                self.search_requests_collection.document(request_id).update({
                    'status': 'completed',
                    'result_count': len(results),
                    'processed_at': datetime.utcnow()
                })
                
                logging.info(f"Processed search request {request_id}: '{query}' found {len(results)} results")
        except Exception as e:
            logging.error(f"Error handling search requests: {e}")

def main(local_mode=False):
    # Get project ID from the service account file or environment
    project_id = os.environ.get('GOOGLE_CLOUD_PROJECT')
    if not project_id and not local_mode:
        try:
            # Try to get from service account file
            cred_file = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
            if cred_file and os.path.exists(cred_file):
                with open(cred_file, 'r') as f:
                    creds = json.load(f)
                    project_id = creds.get('project_id')
        except Exception as e:
            logging.error(f"Error getting project ID: {e}")
    
    if not project_id and not local_mode:
        logging.warning("No project ID found. Using 'local-project' as fallback.")
        project_id = 'local-project'
    
    # Initialize and run the indexer
    indexer = GCPIndexer(project_id=project_id, local_mode=local_mode)
    indexer.run()

if __name__ == '__main__':
    main() 