from flask import Flask, render_template, request, jsonify
import subprocess
import json
import os
from datetime import datetime
from mpi4py import MPI
import threading
import queue
import hashlib
import time
import logging

# Initialize Flask app with the correct template folder
template_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), 'templates'))
app = Flask(__name__, template_folder=template_dir)
crawl_process = None
status_queue = queue.Queue()

def read_crawl_state():
    """Read the current crawl state from file"""
    try:
        with open('crawl_state.json', 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return None
    except Exception as e:
        return {'error': str(e)}

@app.route('/')
def home():
    """Home page with crawl form and status"""
    return render_template('index.html')

@app.route('/start_crawl', methods=['POST'])
def start_crawl():
    """Start a new crawl with the given parameters"""
    global crawl_process
    
    # Get parameters from form
    seed_urls = request.form.get('urls').split('\n')
    seed_urls = [url.strip() for url in seed_urls if url.strip()]
    
    config = {
        'seed_urls': seed_urls,
        'max_depth': int(request.form.get('depth', 3)),
        'max_pages_per_domain': int(request.form.get('limit', 1000)),
        'respect_robots': request.form.get('respect_robots', 'true') == 'true',
        'crawl_delay': float(request.form.get('delay', 1.0)),
        'allowed_domains': request.form.get('allowed_domains', '').split(','),
        'start_time': datetime.now().isoformat()
    }
    
    # Save configuration
    os.makedirs('config', exist_ok=True)
    with open('config/crawl_config.json', 'w') as f:
        json.dump(config, f, indent=2)
    
    # Start the crawl process with MPI
    num_nodes = int(request.form.get('nodes', 3))
    cmd = f"mpiexec -n {num_nodes} python src/run_crawler.py"
    
    try:
        # Kill any existing crawl process
        if crawl_process:
            try:
                crawl_process.terminate()
                crawl_process.wait(timeout=5)
            except:
                pass
        
        # Start new crawl process
        crawl_process = subprocess.Popen(
            cmd.split(),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True
        )
        
        # Wait a bit to check for immediate failures
        time.sleep(2)
        if crawl_process.poll() is not None:
            _, stderr = crawl_process.communicate()
            return jsonify({
                'status': 'error',
                'message': f'Crawl process failed to start: {stderr}'
            })
        
        return jsonify({
            'status': 'success',
            'message': 'Crawl started successfully'
        })
        
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        })

@app.route('/status')
def get_status():
    """Get current crawl status"""
    state = read_crawl_state()
    if state:
        return jsonify(state)
    return jsonify({'status': 'No crawl in progress'})

@app.route('/search', methods=['POST'])
def search():
    """Search the index"""
    query = request.form.get('query')
    field = request.form.get('field', 'content')
    
    if not query:
        return jsonify({'error': 'No query provided'})
    
    try:
        # Check if crawler is running
        if not crawl_process or crawl_process.poll() is not None:
            return jsonify({
                'error': 'Crawler is not running. Please start a crawl first.'
            })
        
        # Save search request to file
        search_request = {
            'query': query,
            'field': field,
            'timestamp': datetime.now().isoformat(),
            'request_id': hashlib.md5(f"{query}{field}{time.time()}".encode()).hexdigest()
        }
        
        os.makedirs('data/search_requests', exist_ok=True)
        request_file = f'data/search_requests/{search_request["request_id"]}.json'
        with open(request_file, 'w') as f:
            json.dump(search_request, f)
        
        # Wait for results (max 5 seconds)
        result_file = f'data/search_results/{search_request["request_id"]}.json'
        start_time = time.time()
        while time.time() - start_time < 5:
            if os.path.exists(result_file):
                with open(result_file, 'r') as f:
                    results = json.load(f)
                os.remove(result_file)  # Clean up
                os.remove(request_file)  # Clean up
                return jsonify(results)
            time.sleep(0.1)
        
        # If no results after timeout
        os.remove(request_file)  # Clean up
        return jsonify({
            'error': 'Search timed out. Please try again.'
        })
        
    except Exception as e:
        logging.error(f"Search error: {e}")
        return jsonify({
            'error': f'Search failed: {str(e)}'
        })

@app.route('/stop_crawl', methods=['POST'])
def stop_crawl():
    """Stop the current crawl"""
    global crawl_process
    
    try:
        if crawl_process:
            # Try graceful termination first
            crawl_process.terminate()
            # Wait up to 5 seconds for process to terminate
            try:
                crawl_process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                # Force kill if process doesn't terminate gracefully
                crawl_process.kill()
                crawl_process.wait()
            
            crawl_process = None
            
            # Update crawl state to indicate stopped status
            try:
                with open('crawl_state.json', 'r') as f:
                    state = json.load(f)
                state['is_running'] = False
                with open('crawl_state.json', 'w') as f:
                    json.dump(state, f)
            except Exception as e:
                print(f"Error updating crawl state: {e}")
            
            return jsonify({'status': 'success', 'message': 'Crawl stopped successfully'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': f'Error stopping crawl: {str(e)}'})
    
    return jsonify({'status': 'error', 'message': 'No crawl in progress'})

@app.route('/get_html', methods=['POST'])
def get_html():
    """Get the HTML content for a specific URL"""
    url = request.form.get('url')
    if not url:
        return jsonify({'error': 'URL not provided'})
    
    try:
        # Try to read from the stored HTML files
        # Convert URL to a safe filename
        url_hash = hashlib.md5(url.encode()).hexdigest()
        html_file = os.path.join('data', 'html', f'{url_hash}.html')
        
        if os.path.exists(html_file):
            with open(html_file, 'r', encoding='utf-8') as f:
                html_content = f.read()
            return jsonify({
                'status': 'success',
                'url': url,
                'html': html_content
            })
        else:
            return jsonify({
                'status': 'error',
                'message': f'HTML content not found for {url}'
            })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        })

@app.route('/check_url', methods=['POST'])
def check_url():
    """Check if a URL has been crawled"""
    url = request.form.get('url')
    if not url:
        return jsonify({'error': 'URL not provided'})
    
    try:
        # Check crawl state file
        state = read_crawl_state()
        if not state:
            return jsonify({'crawled': False, 'message': 'No crawl data available'})
            
        # Check if URL is in completed URLs
        if 'completed' in state and url in state['completed']:
            return jsonify({
                'crawled': True,
                'message': 'URL has been crawled',
                'timestamp': state.get('timestamp', 'Unknown')
            })
            
        # Check if URL is in failed URLs
        if 'failed' in state and url in state['failed']:
            return jsonify({
                'crawled': False,
                'message': 'URL crawling failed',
                'timestamp': state.get('timestamp', 'Unknown')
            })
            
        # Check if URL is in progress
        if 'in_progress' in state and url in state.get('in_progress', {}):
            return jsonify({
                'crawled': False,
                'message': 'URL is currently being crawled',
                'timestamp': state.get('timestamp', 'Unknown')
            })
        
        return jsonify({
            'crawled': False,
            'message': 'URL has not been crawled'
        })
        
    except Exception as e:
        return jsonify({'error': str(e)})

if __name__ == '__main__':
    app.run(debug=True, port=5000)