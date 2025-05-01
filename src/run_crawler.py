import os
import sys
import logging
import logging.handlers

# -------- CLEAR OLD LOGS FIRST -------- #
def clear_old_logs():
    """Delete previous log files if they exist"""
    # Close any existing logging handlers
    for handler in logging.getLogger().handlers[:]:
        handler.close()
        logging.getLogger().removeHandler(handler)
    logging.shutdown()
    
    logs = ['master.log', 'indexer.log', 'crawler.log']
    for log_file in logs:
        if os.path.exists(log_file):
            try:
                os.remove(log_file)
                print(f"Deleted old log: {log_file}")
            except Exception as e:
                print(f"Could not delete {log_file}: {e}")

clear_old_logs()

# -------------------------------------- #

from mpi4py import MPI

def setup_logging(process_type):
    """Setup logging for each process type"""
    log_file = f"{process_type.lower()}.log"
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - ' + process_type + ' - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_file)
        ]
    )

def main():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    
    # Add src directory to Python path
    src_dir = os.path.dirname(os.path.abspath(__file__))
    sys.path.append(src_dir)
    
    if rank == 0:
        setup_logging('Master')
        from master import master_process
        master_process()
    elif rank == size - 1:
        setup_logging('Indexer')
        from indexer import indexer_process
        indexer_process()
    else:
        setup_logging('Crawler')
        from crawler import crawler_process
        crawler_process()  # Call directly, as crawler_process manages its own event loop

if __name__ == '__main__':
    main()