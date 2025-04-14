from mpi4py import MPI
import sys
import os
import logging

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
        # Master process
        setup_logging('Master')
        from master import master_process
        master_process()
    elif rank == size - 1:
        # Last process is indexer
        setup_logging('Indexer')
        from indexer import indexer_process
        indexer_process()
    else:
        # All other processes are crawlers
        setup_logging('Crawler')
        from crawler import crawler_process
        import asyncio
        asyncio.run(crawler_process())

if __name__ == '__main__':
    main() 