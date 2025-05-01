import os
import sys
import logging
import logging.handlers
from mpi4py import MPI

def clear_old_logs():
    """Delete previous log files if they exist (called only by master)"""
    logs = ['master.log', 'indexer.log', 'crawler.log']
    for log_file in logs:
        if os.path.exists(log_file):
            try:
                os.remove(log_file)
                logging.info(f"Deleted old log: {log_file}")
            except Exception as e:
                logging.error(f"Could not delete {log_file}: {e}")

def setup_logging(process_type, rank):
    """Setup logging for each process type"""
    log_file = f"{process_type.lower()}.log"
    # Ensure previous handlers are closed
    for handler in logging.getLogger().handlers[:]:
        handler.close()
        logging.getLogger().removeHandler(handler)
    
    logging.basicConfig(
        level=logging.INFO,
        format=f'%(asctime)s - {process_type} (Rank {rank}) - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_file, mode='w')  # Overwrite log file
        ]
    )

def main():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    
    # Add src directory to Python path
    src_dir = os.path.dirname(os.path.abspath(__file__))
    sys.path.append(src_dir)
    
    # Only master clears old logs
    if rank == 0:
        clear_old_logs()
    
    if rank == 0:
        setup_logging('Master', rank)
        from master import master_process
        master_process()
    elif rank == size - 1:
        setup_logging('Indexer', rank)
        from indexer import indexer_process
        indexer_process()
    else:
        setup_logging('Crawler', rank)
        from crawler import crawler_process
        crawler_process()

if __name__ == '__main__':
    main()