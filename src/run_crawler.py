import os
import sys
import logging
import logging.handlers
import time
from mpi4py import MPI

def check_running_instance():
    """Check if another instance is running using a lock file"""
    lock_file = 'crawler.lock'
    if os.path.exists(lock_file):
        with open(lock_file, 'r') as f:
            pid = f.read().strip()
        logging.error(f"Another instance of the crawler (PID {pid}) is running. Please terminate it first.")
        sys.exit(1)
    
    # Create lock file with current PID
    with open(lock_file, 'w') as f:
        f.write(str(os.getpid()))
    return lock_file

def clear_old_logs():
    """Delete previous log files with retry logic"""
    logs = ['master.log', 'indexer.log', 'crawler.log']
    for log_file in logs:
        if os.path.exists(log_file):
            for attempt in range(3):
                try:
                    os.remove(log_file)
                    logging.info(f"Deleted old log: {log_file}")
                    break
                except PermissionError as e:
                    logging.warning(f"Attempt {attempt + 1}: Could not delete {log_file}: {e}")
                    time.sleep(1)  # Wait before retrying
                except Exception as e:
                    logging.error(f"Could not delete {log_file}: {e}")
                    break

def setup_logging(process_type, rank):
    """Setup logging for each process type with fallback for PermissionError"""
    log_file = f"{process_type.lower()}.log"
    # Close any existing handlers
    for handler in logging.getLogger().handlers[:]:
        handler.close()
        logging.getLogger().removeHandler(handler)
    
    handlers = [logging.StreamHandler()]
    try:
        handlers.append(logging.FileHandler(log_file, mode='w'))
    except PermissionError as e:
        fallback_log = f"{process_type.lower()}_fallback_{rank}.log"
        logging.warning(f"Permission denied for {log_file}: {e}. Falling back to {fallback_log}")
        handlers.append(logging.FileHandler(fallback_log, mode='w'))
    
    logging.basicConfig(
        level=logging.INFO,
        format=f'%(asctime)s - {process_type} (Rank {rank}) - %(levelname)s - %(message)s',
        handlers=handlers
    )

def main():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    
    # Add src directory to Python path
    src_dir = os.path.dirname(os.path.abspath(__file__))
    sys.path.append(src_dir)
    
    # Check for running instance
    lock_file = None
    if rank == 0:
        lock_file = check_running_instance()
        clear_old_logs()
    
    try:
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
    finally:
        # Remove lock file only if master process created it
        if rank == 0 and lock_file and os.path.exists(lock_file):
            try:
                os.remove(lock_file)
                logging.info("Removed lock file")
            except Exception as e:
                logging.error(f"Could not remove lock file {lock_file}: {e}")

if __name__ == '__main__':
    main()