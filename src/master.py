from mpi4py import MPI
import time
import logging

# Import necessary libraries for task queue, database, etc. (e.g., redis,
# cloud storage SDKs)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - Master - %(levelname)s - %(message)s')

def master_process():
    """
    Main process for the master node.
    Handles task distribution, worker management, and coordination.
    """
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    status = MPI.Status()
    logging.info(f"Master node started with rank {rank} of {size}")

    # Initialize task queue, database connections, etc.
    # ... (Implementation needed) ...
    
    crawler_nodes = size - 2  # Assuming master and at least one indexer node
    indexer_nodes = 1  # At least one indexer node
    
    if crawler_nodes <= 0 or indexer_nodes <= 0:
        logging.error("Not enough nodes to run crawler and indexer. Need at least 3 nodes (1 master, 1 crawler, 1 indexer)")
        return

    active_crawler_nodes = list(range(1, 1 + crawler_nodes))  # Ranks for crawler nodes (assuming rank 0 is master)
    active_indexer_nodes = list(range(1 + crawler_nodes, size))  # Ranks for indexer nodes
    
    logging.info(f"Active Crawler Nodes: {active_crawler_nodes}")
    logging.info(f"Active Indexer Nodes: {active_indexer_nodes}")

    seed_urls = ["http://example.com", "http://example.org"]  # Example seed URLs - replace with actual seed URLs
    urls_to_crawl_queue = seed_urls  # Simple list as initial queue - replace with a distributed queue
    task_count = 0
    crawler_tasks_assigned = 0

    while urls_to_crawl_queue or crawler_tasks_assigned > 0:  # Continue as long as there are URLs to crawl or tasks in progress
        # Check for completed crawler tasks and results from crawler nodes
        if crawler_tasks_assigned > 0:
            if comm.iprobe(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status):  # Non-blocking check for incoming messages
                message_source = status.Get_source()
                message_tag = status.Get_tag()
                message_data = comm.recv(source=message_source, tag=message_tag)

                if message_tag == 1:  # Crawler completed task and sent back extracted URLs
                    crawler_tasks_assigned -= 1
                    new_urls = message_data  # Assuming message_data is a list of URLs
                    if new_urls:
                        urls_to_crawl_queue.extend(new_urls)  # Add newly discovered URLs to the queue
                    logging.info(f"Master received URLs from Crawler {message_source}, URLs in queue: {len(urls_to_crawl_queue)}, Tasks assigned: {crawler_tasks_assigned}")
                elif message_tag == 99:  # Crawler node reports status/heartbeat
                    logging.info(f"Crawler {message_source} status: {message_data}")  # Example status message
                elif message_tag == 999:  # Crawler node reports error
                    logging.error(f"Crawler {message_source} reported error: {message_data}")
                    crawler_tasks_assigned -= 1  # Decrement task count even on error, consider re-assigning task in real implementation

        # Assign new crawling tasks if there are URLs in the queue and available crawler nodes
        while urls_to_crawl_queue and crawler_tasks_assigned < crawler_nodes:  # Limit tasks to available crawler nodes for simplicity in this skeleton
            url_to_crawl = urls_to_crawl_queue.pop(0)  # Get URL from queue (FIFO for simplicity)
            available_crawler_rank = active_crawler_nodes[crawler_tasks_assigned % len(active_crawler_nodes)]  # Simple round-robin assignment
            task_id = task_count
            task_count += 1
            comm.send(url_to_crawl, dest=available_crawler_rank, tag=0)  # Tag 0 for task assignment
            crawler_tasks_assigned += 1
            logging.info(f"Master assigned task {task_id} (crawl {url_to_crawl}) to Crawler {available_crawler_rank}, Tasks assigned: {crawler_tasks_assigned}")

        time.sleep(0.1)  # Small delay to prevent overwhelming master in this example

    time.sleep(1)  # Master node's main loop sleep - adjust as needed
    logging.info("Master node finished URL distribution. Waiting for crawlers to complete...")
    # In a real system, you would have more sophisticated shutdown and result aggregation logic
    print("Master Node Finished.")

if __name__ == '__main__':
    master_process()