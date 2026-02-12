from flask import Flask, render_template, request, jsonify, redirect
import subprocess
import json
import os
from datetime import datetime
import threading
import queue
import hashlib
import time
import logging
import sys
import re
import signal

# Initialize Flask app
template_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), 'templates'))
static_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), 'static'))
app = Flask(__name__, template_folder=template_dir, static_folder=static_dir)
crawl_process = None
status_queue = queue.Queue()

def read_crawl_state():
    """Read the current crawl state from file"""
    # Get the root directory (one level up from templates)
    root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    # Use the same path as the crawler: data/state/current_state.json
    state_file = os.path.join(root_dir, 'data', 'state', 'current_state.json')
    
    try:
        if not os.path.exists(state_file):
            return None
        with open(state_file, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error reading state file {state_file}: {e}")
        return {'error': str(e)}

@app.route('/')
def home():
    """Home page with crawl form and status"""
    return render_template('index.html')

def get_start_crawl_success_html(message):
    """Generate a nice HTML response for successful crawl start"""
    return f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Crawl Started</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 0; padding: 0; background-color: white; }}
            .header {{ 
                background-color: #4CAF50; 
                color: white; 
                padding: 10px;
                margin: 0; 
                font-size: 24px;
                font-weight: bold;
            }}
            .content {{
                background-color: #E8F5E9;
                padding: 15px;
                margin: 0;
                border-radius: 0;
            }}
            .container {{
                border: 1px solid #ddd;
                border-radius: 5px;
                margin: 20px;
                overflow: hidden;
            }}
            .button {{
                display: inline-block;
                background-color: #4CAF50;
                color: white;
                padding: 10px 20px;
                text-decoration: none;
                border-radius: 4px;
                margin-top: 15px;
            }}
            .button:hover {{
                background-color: #45a049;
            }}
        </style>
        <meta http-equiv="refresh" content="3;url=/">
    </head>
    <body>
        <div class="container">
            <div class="header">Crawl Started</div>
            <div class="content">
                <p>{message}</p>
                <p>Redirecting to homepage in 3 seconds...</p>
                <a href="/" class="button">Return to homepage</a>
            </div>
        </div>
    </body>
    </html>
    """

def get_start_crawl_error_html(message):
    """Generate a nice HTML response for crawl start error"""
    return f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Crawl Error</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 0; padding: 0; background-color: white; }}
            .header {{ 
                background-color: #F44336; 
                color: white; 
                padding: 10px;
                margin: 0; 
                font-size: 24px;
                font-weight: bold;
            }}
            .content {{
                background-color: #FFEBEE;
                padding: 15px;
                margin: 0;
                border-radius: 0;
            }}
            .container {{
                border: 1px solid #ddd;
                border-radius: 5px;
                margin: 20px;
                overflow: hidden;
            }}
            .button {{
                display: inline-block;
                background-color: #F44336;
                color: white;
                padding: 10px 20px;
                text-decoration: none;
                border-radius: 4px;
                margin-top: 15px;
            }}
            .button:hover {{
                background-color: #d32f2f;
            }}
        </style>
        <meta http-equiv="refresh" content="5;url=/">
    </head>
    <body>
        <div class="container">
            <div class="header">Error Starting Crawl</div>
            <div class="content">
                <p>{message}</p>
                <p>Redirecting to homepage in 5 seconds...</p>
                <a href="/" class="button">Return to homepage</a>
            </div>
        </div>
    </body>
    </html>
    """

@app.route('/start_crawl', methods=['POST'])
def start_crawl():
    """Start a new crawl with the given parameters or add URLs to existing crawl"""
    global crawl_process
    
    # Check if the client wants HTML or JSON (default to JSON for API compatibility)
    wants_html = request.args.get('html', 'false').lower() == 'true'
    # Always treat direct browser form submission as wanting HTML
    if request.content_type and 'application/x-www-form-urlencoded' in request.content_type:
        wants_html = True

    # Get parameters from form
    seed_urls = request.form.get('urls').split('\n')
    seed_urls = [url.strip() for url in seed_urls if url.strip()]

    # Get the root directory (one level up from templates)
    root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    
    # Check if a crawl is already running
    # Use the correct state file path
    state_file = os.path.join(root_dir, 'data', 'state', 'current_state.json')
    crawl_already_running = False
    existing_state = None
    
    if os.path.exists(state_file):
        try:
            with open(state_file, 'r') as f:
                existing_state = json.load(f)
                # Note: master writes is_running: True
                if existing_state.get('is_running', False):
                    crawl_already_running = True
        except Exception as e:
            print(f"Error reading crawl state at {state_file}: {e}")
    
    # Prepare configuration
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
    config_dir = os.path.join(root_dir, 'config')
    os.makedirs(config_dir, exist_ok=True)
    
    # If a crawl is already running, add the new URLs to the queue
    if crawl_already_running and existing_state:
        try:
            print(f"Crawl already running. Adding {len(seed_urls)} new URLs to the queue.")
            
            # Add new URLs to the state's queue
            if 'queue' not in existing_state:
                existing_state['queue'] = []
                
            # Add URLs that aren't already in queue, completed, or in_progress
            queue_set = set(existing_state.get('queue', []))
            completed_set = set(existing_state.get('completed', []))
            in_progress_set = set(existing_state.get('in_progress', {}).keys())
            failed_set = set(existing_state.get('failed', []))
            
            added_urls = []
            for url in seed_urls:
                if url not in queue_set and url not in completed_set and url not in in_progress_set and url not in failed_set:
                    existing_state['queue'].append(url)
                    added_urls.append(url)
            
            # Update the state file
            with open(state_file, 'w') as f:
                json.dump(existing_state, f, indent=2)
                
            # Update the crawl configuration with new URLs
            with open(os.path.join(config_dir, 'crawl_config.json'), 'r') as f:
                current_config = json.load(f)
                
            # Add new URLs to existing seed URLs (avoid duplicates)
            existing_seeds = set(current_config.get('seed_urls', []))
            for url in seed_urls:
                if url not in existing_seeds:
                    current_config.setdefault('seed_urls', []).append(url)
                    
            # Save updated config
            with open(os.path.join(config_dir, 'crawl_config.json'), 'w') as f:
                json.dump(current_config, f, indent=2)
            
            message = f'Added {len(added_urls)} new URLs to the existing crawl queue'
            if wants_html:
                return get_start_crawl_success_html(message)
            else:
                return jsonify({
                    'status': 'success',
                    'message': message,
                    'added_urls': added_urls
                })
            
        except Exception as e:
            print(f"Error adding URLs to existing crawl: {e}")
            # Continue with normal crawl start if there was an error
    
    # If no crawl is running, save new configuration
    with open(os.path.join(config_dir, 'crawl_config.json'), 'w') as f:
        json.dump(config, f, indent=2)

    # Check if local mode is requested
    use_local_mode = request.form.get('local_mode', 'false') == 'true'
    
    # Get the root directory (one level up from templates)
    root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    script_path = os.path.join(root_dir, 'src', 'run_gcloud_crawler.py')
    
    # Check for and remove any existing lock file
    lock_file = os.path.join(root_dir, 'crawler.lock')
    if os.path.exists(lock_file):
        try:
            print(f"Removing existing lock file: {lock_file}")
            os.remove(lock_file)
        except Exception as e:
            print(f"Warning: Failed to remove lock file: {e}")
    
    # Verify the script exists
    if not os.path.exists(script_path):
        message = f'Crawler script not found at: {script_path}'
        if wants_html:
            return get_start_crawl_error_html(message)
        else:
            return jsonify({
                'status': 'error',
                'message': message
            })
    
    try:
        # Try using a different approach for Windows
        if os.name == 'nt':  # Windows
            # Kill any existing process
            if crawl_process:
                try:
                    crawl_process.terminate()
                    crawl_process.wait(timeout=5)
                except:
                    pass
                    
            # Use subprocess.Popen with shell=True and full command string
            print("Starting crawler with subprocess.Popen and shell=True")
            # Build command with proper escaping
            python_path = sys.executable  # Get the current Python interpreter path
            cmd_str = f'"{python_path}" "{script_path}" all'
            if use_local_mode:
                cmd_str += " --local"
                
            # Start the process
            try:
                crawl_process = subprocess.Popen(
                    cmd_str,
                    shell=True,
                    cwd=root_dir,  # Set working directory explicitly
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    universal_newlines=True
                )
                
                # Wait briefly to check for immediate failure
                time.sleep(2)
                if crawl_process.poll() is not None:
                    exit_code = crawl_process.poll()
                    stdout, stderr = crawl_process.communicate()
                    error_msg = f"Crawler process exited with code {exit_code}"
                    if stderr:
                        error_msg += f": {stderr}"
                    if stdout:
                        error_msg += f" Output: {stdout}"
                    
                    print(f"ERROR: {error_msg}")
                    print(f"Command attempted: {cmd_str}")
                    
                    # Check if the error is about another instance running
                    if "Another instance of the crawler" in stderr or "Another instance of the crawler" in stdout:
                        # Extract PID from the error message
                        pid_match = re.search(r"PID (\d+)", stderr or stdout)
                        if pid_match:
                            pid = int(pid_match.group(1))
                            try:
                                print(f"Attempting to terminate existing crawler process with PID {pid}")
                                # Try to kill the process using taskkill on Windows
                                if os.name == 'nt':
                                    try:
                                        # Use taskkill which is more reliable on Windows
                                        subprocess.run(f'taskkill /F /PID {pid}', shell=True)
                                        print(f"Taskkill executed for PID {pid}")
                                    except Exception as taskkill_err:
                                        print(f"Taskkill error: {taskkill_err}")
                                else:
                                    # Unix-like systems
                                    os.kill(pid, signal.SIGTERM)
                                    
                                # Delete the lock file again just to be sure
                                if os.path.exists(lock_file):
                                    try:
                                        os.remove(lock_file)
                                        print(f"Lock file removed after killing process")
                                    except Exception as e:
                                        print(f"Warning: Failed to remove lock file after killing process: {e}")
                                        
                                time.sleep(1)  # Wait for process to terminate
                                
                                # Try again with the crawler
                                message = 'Previous crawler instance was terminated. Please try again.'
                                if wants_html:
                                    return get_start_crawl_error_html(message)
                                else:
                                    return jsonify({
                                        'status': 'retry',
                                        'message': message
                                    })
                            except Exception as kill_err:
                                print(f"Failed to kill process: {kill_err}")
                    
                    message = error_msg
                    if wants_html:
                        return get_start_crawl_error_html(message)
                    else:
                        return jsonify({
                            'status': 'error',
                            'message': message
                        })
                
                # Process started successfully
                message = 'Crawl started successfully'
                if wants_html:
                    return get_start_crawl_success_html(message)
                else:
                    return jsonify({
                        'status': 'success',
                        'message': message
                    })
                
            except Exception as e:
                # If subprocess fails, try direct import approach as a last resort
                print(f"Failed to start process with subprocess: {e}")
                print("Trying direct import approach...")
                
                try:
                    # Add src directory to path so we can import the crawler module
                    src_dir = os.path.join(root_dir, 'src')
                    if src_dir not in sys.path:
                        sys.path.append(src_dir)
                    
                    # Import the module and run it in a thread
                    import importlib.util
                    spec = importlib.util.spec_from_file_location("run_gcloud_crawler", script_path)
                    crawler_module = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(crawler_module)
                    
                    # Start the crawler in a separate thread
                    def run_crawler():
                        if use_local_mode:
                            sys.argv = ["run_gcloud_crawler.py", "all", "--local"]
                        else:
                            sys.argv = ["run_gcloud_crawler.py", "all"]
                        crawler_module.main()
                    
                    crawler_thread = threading.Thread(target=run_crawler)
                    crawler_thread.daemon = True
                    crawler_thread.start()
                    
                    return jsonify({
                        'status': 'success',
                        'message': 'Crawl started successfully using direct import'
                    })
                    
                except Exception as import_err:
                    print(f"Failed to start process using direct import: {import_err}")
                    message = f'Failed to start crawler: {str(e)} and direct import failed: {str(import_err)}'
                    if wants_html:
                        return get_start_crawl_error_html(message)
                    else:
                        return jsonify({
                            'status': 'error',
                            'message': message
                        })
                
        # Original approach for non-Windows platforms
        else:
            # Build command
            cmd = f'python {script_path} all'
            if use_local_mode:
                cmd += " --local"
                
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

            # Wait to check for immediate failures
            time.sleep(3)
            if crawl_process.poll() is not None:
                stdout, stderr = crawl_process.communicate()
                message = f"Crawl process failed to start: {stderr}"
                if wants_html:
                    return get_start_crawl_error_html(message)
                else:
                    return jsonify({
                        'status': 'error',
                        'message': message
                    })

            message = 'Crawl started successfully'
            if wants_html:
                return get_start_crawl_success_html(message)
            else:
                return jsonify({
                    'status': 'success',
                    'message': message
                })

    except Exception as e:
        message = f'Error starting crawler: {str(e)}'
        if wants_html:
            return get_start_crawl_error_html(message)
        else:
            return jsonify({
                'status': 'error',
                'message': message
            })

def get_crawler_status_html():
    """Helper function to generate crawler status HTML without refresh button"""
    state = read_crawl_state()
    
    status_message = 'No crawl in progress'
    if state:
        status_message = 'Crawl in progress' if state.get('is_running', False) else 'Crawl paused'
    
    return f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Crawler Status</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 0; padding: 0; background-color: white; }}
            .header {{ 
                background-color: #00BFFF; 
                color: white; 
                padding: 10px;
                margin: 0; 
                font-size: 24px;
                font-weight: bold;
            }}
            .status-content {{
                background-color: #E0F7FA;
                padding: 15px;
                margin: 0;
                border-radius: 0;
            }}
            .container {{
                border: 1px solid #ddd;
                border-radius: 5px;
                margin: 20px;
                overflow: hidden;
            }}
        </style>
        <meta http-equiv="refresh" content="30">
    </head>
    <body>
        <div class="container">
            <div class="header">Crawler Status</div>
            <div class="status-content">{status_message}</div>
        </div>
    </body>
    </html>
    """

@app.route('/Crawler Status')
def crawler_status_redirect():
    """Handle the /Crawler Status URL directly with a minimal UI"""
    return get_crawler_status_html()

# Multiple routes for all possible URL variations
@app.route('/crawler status', methods=['GET'])
@app.route('/Crawler status', methods=['GET'])
@app.route('/crawler-status', methods=['GET'])
@app.route('/crawlerstatus', methods=['GET'])
@app.route('/crawler_status', methods=['GET'])
def crawler_status():
    """Render the crawler status page with multiple URL patterns"""
    return get_crawler_status_html()

@app.route('/status')
def get_status():
    """Get the current status of the crawl"""
    state = read_crawl_state()
    
    if state is None:
        return jsonify({
            'status': 'No crawl in progress',
            'is_running': False
        })
    
    # Get recent activity
    recent_activity = []
    
    # Check if there are recent finished URLs
    if 'completed' in state and state['completed']:
        # Get the 5 most recent completed URLs
        recent_completed = state['completed'][-5:]
        for url in recent_completed:
            recent_activity.append(f"Completed: {url}")
    
    # Check in-progress URLs
    if 'in_progress' in state and state['in_progress']:
        in_progress = state['in_progress']
        if isinstance(in_progress, dict):
            for url, start_time in in_progress.items():
                recent_activity.append(f"In progress: {url}")
        elif isinstance(in_progress, list):
            for url in in_progress:
                recent_activity.append(f"In progress: {url}")
    
    # If adding URLs to an existing crawl, show that info
    if 'recently_added' in state and state['recently_added']:
        for url in state['recently_added']:
            recent_activity.append(f"Added new URL: {url}")
    
    return jsonify({
        'status': 'Crawl in progress' if state.get('is_running', False) else 'Crawl paused',
        'is_running': state.get('is_running', False),
        'queue_size': len(state.get('queue', [])),
        'in_progress': state.get('in_progress', {}),
        'completed': state.get('completed', []),
        'failed': state.get('failed', []),
        'recent_activity': recent_activity
    })

@app.route('/status_page')
def status_page():
    """Render a simple status page without refresh button"""
    return get_crawler_status_html()

@app.route('/search', methods=['POST'])
def search():
    """Search the index (Optimized Local Implementation)"""
    query = request.form.get('query')
    field = request.form.get('field', 'content')

    if not query:
        return jsonify({'error': 'No query provided'})

    try:
        # Get root directory
        root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        index_file = os.path.join(root_dir, 'data', 'index', 'latest_index.json')
        
        if not os.path.exists(index_file):
            return jsonify({'error': 'Index file not found. Please crawl some pages first.'})

        with open(index_file, 'r') as f:
            index_data = json.load(f)
        
        # Simple local search logic (from cli.py)
        query_terms = re.findall(r'\w+', query.lower())
        if not query_terms:
            return jsonify([])

        scores = {} # url -> score
        url_metadata = {} # url -> {title, timestamp}

        content_index = index_data.get('content_index', {})
        
        for term in query_terms:
            if term in content_index:
                for entry in content_index[term]:
                    url = entry[0]
                    score = entry[1]
                    title = entry[2]
                    timestamp = entry[3]
                    
                    scores[url] = scores.get(url, 0.0) + score
                    url_metadata[url] = {'title': title, 'timestamp': timestamp}

        results = []
        for url, score in scores.items():
            meta = url_metadata[url]
            # Basic filtering
            if field == 'title' and not any(t in meta['title'].lower() for t in query_terms):
                continue
            if field == 'url' and not any(t in url.lower() for t in query_terms):
                continue
            
            results.append({
                'url': url,
                'score': score,
                'title': meta['title'],
                'snippet': f"Score: {score:.2f}", # Simple snippet for now
                'timestamp': meta['timestamp']
            })

        results.sort(key=lambda x: x['score'], reverse=True)
        top_results = results[:20]

        return jsonify(top_results)

    except Exception as e:
        logging.error(f"Search error: {e}")
        return jsonify({'error': f'Search failed: {str(e)}'})

def get_snippet(text, query, chars=150):
    """Extract a snippet of text around the query"""
    try:
        text = text.replace('\n', ' ').replace('\r', '')
        pos = text.lower().find(query.lower())
        if pos == -1:
            return text[:chars] + "..."
            
        start = max(0, pos - chars//2)
        end = min(len(text), pos + len(query) + chars//2)
        
        snippet = text[start:end]
        if start > 0:
            snippet = "..." + snippet
        if end < len(text):
            snippet = snippet + "..."
            
        return snippet
    except Exception:
        return text[:chars] + "..."

def get_stop_crawl_success_html(message):
    """Generate a nice HTML response for successful crawl stop"""
    return f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Crawl Stopped</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 0; padding: 0; background-color: white; }}
            .header {{ 
                background-color: #FF9800; 
                color: white; 
                padding: 10px;
                margin: 0; 
                font-size: 24px;
                font-weight: bold;
            }}
            .content {{
                background-color: #FFF3E0;
                padding: 15px;
                margin: 0;
                border-radius: 0;
            }}
            .container {{
                border: 1px solid #ddd;
                border-radius: 5px;
                margin: 20px;
                overflow: hidden;
            }}
            .button {{
                display: inline-block;
                background-color: #FF9800;
                color: white;
                padding: 10px 20px;
                text-decoration: none;
                border-radius: 4px;
                margin-top: 15px;
            }}
            .button:hover {{
                background-color: #F57C00;
            }}
        </style>
        <meta http-equiv="refresh" content="3;url=/">
    </head>
    <body>
        <div class="container">
            <div class="header">Crawl Stopped</div>
            <div class="content">
                <p>{message}</p>
                <p>Redirecting to homepage in 3 seconds...</p>
                <a href="/" class="button">Return to homepage</a>
            </div>
        </div>
    </body>
    </html>
    """

@app.route('/stop_crawl', methods=['POST'])
def stop_crawl():
    """Stop the current crawl"""
    global crawl_process
    
    # Check if the client wants HTML or JSON (default to JSON for API compatibility)
    wants_html = request.args.get('html', 'false').lower() == 'true'
    # Always treat direct browser form submission as wanting HTML
    if request.content_type and 'application/x-www-form-urlencoded' in request.content_type:
        wants_html = True

    try:
        if crawl_process:
            crawl_process.terminate()
            try:
                crawl_process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                crawl_process.kill()
                crawl_process.wait()

            crawl_process = None

            # Update crawl state
            try:
                root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
                state_file = os.path.join(root_dir, 'data', 'state', 'current_state.json')
                if os.path.exists(state_file):
                    with open(state_file, 'r') as f:
                        state = json.load(f)
                    state['is_running'] = False
                    with open(state_file, 'w') as f:
                        json.dump(state, f, indent=2)
            except Exception as e:
                logging.error(f"Error updating crawl state: {e}")

            message = 'Crawl stopped successfully'
            if wants_html:
                return get_stop_crawl_success_html(message)
            else:
                return jsonify({'status': 'success', 'message': message})

    except Exception as e:
        message = f'Error stopping crawl: {str(e)}'
        if wants_html:
            return get_start_crawl_error_html(message)  # Reuse the error template
        else:
            return jsonify({'status': 'error', 'message': message})

    message = 'No crawl in progress'
    if wants_html:
        return get_stop_crawl_success_html(message)
    else:
        return jsonify({'status': 'error', 'message': message})

@app.route('/get_html', methods=['POST'])
def get_html():
    """Get the HTML content for a specific URL"""
    url = request.form.get('url')
    if not url:
        return jsonify({'error': 'URL not provided'})

    try:
        # Get the root directory path
        root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        
        # Convert URL to filename format (similar to how crawler stores files)
        url_filename = url.replace('https://', 'https_').replace('http://', 'http_').replace('/', '_')
        if not url_filename.endswith('.html'):
            url_filename += '.html'
        
        # First check in the html directory
        html_dir = os.path.join(root_dir, 'data', 'html')
        if os.path.exists(html_dir):
            # Try direct filename match
            html_file = os.path.join(html_dir, url_filename)
            if os.path.exists(html_file):
                with open(html_file, 'r', encoding='utf-8', errors='replace') as f:
                    html_content = f.read()
                return jsonify({
                    'status': 'success',
                    'url': url, 
                    'html': html_content,
                    'source': 'html_file'
                })
            
            # Try to find a file that contains the URL domain
            url_domain = url.replace('https://', '').replace('http://', '').split('/')[0]
            for filename in os.listdir(html_dir):
                if url_domain in filename:
                    # If URL has a specific path, check if that path is in the filename
                    path_parts = url.split('/')
                    if len(path_parts) > 3:  # Has path beyond domain
                        path = '/'.join(path_parts[3:])
                        if path.replace('/', '_') in filename:
                            file_path = os.path.join(html_dir, filename)
                            with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                                html_content = f.read()
                            return jsonify({
                                'status': 'success',
                                'url': url, 
                                'html': html_content,
                                'source': 'html_file'
                            })
        
        # Try MD5 hash lookup as a fallback
        url_hash = hashlib.md5(url.encode()).hexdigest()
        html_file = os.path.join(html_dir, f'{url_hash}.html')
        if os.path.exists(html_file):
            with open(html_file, 'r', encoding='utf-8', errors='replace') as f:
                html_content = f.read()
            return jsonify({
                'status': 'success',
                'url': url, 
                'html': html_content,
                'source': 'html_file'
            })
            
        # Next check in the crawled directory for processed content
        crawled_dir = os.path.join(root_dir, 'data', 'crawled')
        if os.path.exists(crawled_dir):
            # Look for files with matching URL
            for file in os.listdir(crawled_dir):
                if file.endswith('.json'):
                    try:
                        with open(os.path.join(crawled_dir, file), 'r', encoding='utf-8', errors='replace') as f:
                            data = json.load(f)
                            if data.get('url') == url:
                                # Check if HTML or text content is available
                                content = data.get('html', data.get('content', data.get('text', '')))
                                if content:
                                    return jsonify({
                                        'status': 'success',
                                        'url': url,
                                        'html': content,
                                        'source': 'crawled_file',
                                        'is_text': 'html' not in data
                                    })
                    except Exception as e:
                        print(f"Error reading crawled file {file}: {e}")
                        continue
                        
        return jsonify({
            'status': 'error',
            'message': f'HTML content not found for {url}. Has this URL been crawled?'
        })

    except Exception as e:
        logging.error(f"Get HTML error: {e}")
        return jsonify({
            'status': 'error',
            'message': f'Error retrieving content: {str(e)}'
        })

@app.route('/check_url', methods=['POST'])
def check_url():
    """Check if a URL has been crawled"""
    url = request.form.get('url')
    if not url:
        return jsonify({'error': 'URL not provided'})

    try:
        # Get the root directory path
        root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        
        # Check different data directories
        crawled_dir = os.path.join(root_dir, 'data', 'crawled')
        html_dir = os.path.join(root_dir, 'data', 'html')
        
        # Convert URL to a filename format (similar to how crawler stores files)
        url_filename = url.replace('https://', 'https_').replace('http://', 'http_').replace('/', '_')
        if not url_filename.endswith('.html'):
            url_filename += '.html'
        
        # Check if URL has been crawled and stored as HTML
        if os.path.exists(html_dir):
            html_file = os.path.join(html_dir, url_filename)
            if os.path.exists(html_file):
                return jsonify({
                    'crawled': True,
                    'message': 'URL has been crawled and raw HTML is available',
                    'location': 'html'
                })
            
            # Also check for alternate format that might have been used
            for filename in os.listdir(html_dir):
                # This handles cases where URL might be encoded differently
                if url.replace('https://', '') in filename or url.replace('http://', '') in filename:
                    return jsonify({
                        'crawled': True,
                        'message': 'URL has been crawled and raw HTML is available',
                        'location': 'html'
                    })
        
        # Convert URL to a hash for filename lookup as a fallback
        url_hash = hashlib.md5(url.encode()).hexdigest()
        
        # Check if URL has been crawled and stored as HTML with hash
        if os.path.exists(html_dir):
            html_file = os.path.join(html_dir, f'{url_hash}.html')
            if os.path.exists(html_file):
                return jsonify({
                    'crawled': True,
                    'message': 'URL has been crawled and raw HTML is available',
                    'location': 'html'
                })
        
        # Check if URL has been processed and stored in crawled directory
        if os.path.exists(crawled_dir):
            # Look for files with matching URL or hash
            for file in os.listdir(crawled_dir):
                if file.endswith('.json'):
                    try:
                        with open(os.path.join(crawled_dir, file), 'r', encoding='utf-8', errors='replace') as f:
                            data = json.load(f)
                            if data.get('url') == url:
                                return jsonify({
                                    'crawled': True,
                                    'message': 'URL has been crawled and processed',
                                    'location': 'crawled'
                                })
                    except Exception as e:
                        print(f"Error reading crawled file {file}: {e}")
                        continue
                        
        # Check crawler state as a last resort
        state = read_crawl_state()
        if state:
            if 'completed' in state and url in state.get('completed', []):
                return jsonify({
                    'crawled': True, 
                    'message': 'URL has been crawled according to crawler state', 
                    'location': 'state'
                })

            if 'failed' in state and url in state.get('failed', []):
                return jsonify({
                    'crawled': False, 
                    'message': 'URL crawling failed', 
                    'location': 'state'
                })

            if 'in_progress' in state and url in state.get('in_progress', {}):
                return jsonify({
                    'crawled': 'in_progress', 
                    'message': 'URL is currently being crawled', 
                    'location': 'state'
                })
        
        # Check if crawler is running to give helpful feedback
        if crawl_process and crawl_process.poll() is None:
            return jsonify({
                'crawled': False, 
                'message': 'URL has not been crawled yet, but crawler is running'
            })
        else:
            return jsonify({
                'crawled': False, 
                'message': 'URL has not been crawled. Start a crawl first.'
            })

    except Exception as e:
        logging.error(f"Check URL error: {e}")
        return jsonify({'error': str(e)})

@app.route('/<path:path>')
def catch_all(path):
    """Catch-all route to handle any URL that might be related to crawler status"""
    path_lower = path.lower()
    if 'crawler' in path_lower and 'status' in path_lower:
        # If the URL seems to be asking for crawler status, redirect to our clean route
        return redirect('/crawler_status')
    
    # For any other unknown URL, return a 404 error
    return "Page not found", 404

@app.route('/get_logs')
def get_logs():
    """Get recent logs from crawler components"""
    since = request.args.get('since', '0')
    try:
        since_timestamp = float(since)
    except ValueError:
        since_timestamp = 0
    
    # Get the root directory path
    root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    templates_dir = os.path.abspath(os.path.dirname(__file__))
    
    logs = []
    
    # Set up log files to check - check project root dir
    log_file_paths = {
        'master': [os.path.join(root_dir, 'gcpmaster.log')],
        'crawler': [os.path.join(root_dir, 'gcpcrawler.log')],
        'indexer': [os.path.join(root_dir, 'gcpindexer.log')]
    }
    
    # Debug output
    print(f"Checking for logs in root_dir: {root_dir}")
    log_files_found = []
    
    # Parse logs from all possible locations
    for log_type, file_paths in log_file_paths.items():
        for log_path in file_paths:
            print(f"Looking for {log_type} log at: {log_path}, exists: {os.path.exists(log_path)}")
            
            if not os.path.exists(log_path):
                continue
                
            log_files_found.append(log_path)
            
            try:
                with open(log_path, 'r', encoding='utf-8', errors='replace') as f:
                    log_content = f.readlines()
                
                for line in log_content:
                    line = line.strip()
                    if not line:
                        continue
                        
                    # Attempt to parse timestamp in format: 2025-05-15 18:29:40,766 - Master - INFO
                    timestamp_match = re.search(r'(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})', line)
                    if timestamp_match:
                        try:
                            timestamp_str = timestamp_match.group(1)
                            log_time = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
                            log_timestamp = log_time.timestamp()
                            
                            if log_timestamp > since_timestamp:
                                logs.append({
                                    'type': log_type,
                                    'timestamp': log_timestamp,
                                    'message': line
                                })
                            continue
                        except Exception as e:
                            print(f"Error parsing timestamp: {timestamp_str}, {e}")
                    
                    # Try alternate format with brackets [yyyy-mm-dd HH:MM:SS]
                    alt_match = re.search(r'\[([\d-]+\s+[\d:]+)\]', line)
                    if alt_match:
                        try:
                            timestamp_str = alt_match.group(1)
                            log_time = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
                            log_timestamp = log_time.timestamp()
                            
                            if log_timestamp > since_timestamp:
                                logs.append({
                                    'type': log_type,
                                    'timestamp': log_timestamp,
                                    'message': line
                                })
                            continue
                        except Exception as e:
                            print(f"Error parsing alternate timestamp: {timestamp_str}, {e}")
                    
                    # If no timestamp parsing worked but we're doing initial load, include the line anyway
                    if since_timestamp == 0:
                        logs.append({
                            'type': log_type,
                            'timestamp': time.time(),
                            'message': line
                        })
            except Exception as e:
                print(f"Error reading log file {log_path}: {e}")
                logs.append({
                    'type': 'error',
                    'timestamp': time.time(),
                    'message': f"Error reading {log_type} log: {str(e)}"
                })
    
    # Add diagnostic log if no logs found
    if not logs:
        logs.append({
            'type': 'system',
            'timestamp': time.time(),
            'message': f"No logs found. Please check log files. Searched in: {root_dir}"
        })
    else:
        print(f"Found {len(logs)} log entries from: {log_files_found}")
    
    # Sort logs by timestamp
    logs.sort(key=lambda x: x['timestamp'])
    
    # Limit the number of logs to return
    if len(logs) > 200:
        logs = logs[-200:]
    
    return jsonify({
        'logs': logs,
        'timestamp': time.time()
    })

@app.route('/debug_logs')
def debug_logs():
    """Debug endpoint to help troubleshoot log issues"""
    # Get the root directory path
    root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    
    result = {
        'working_directory': os.getcwd(),
        'root_directory': root_dir,
        'log_files': {}
    }
    
    # Check for log files
    log_files = ['gcpmaster.log', 'gcpcrawler.log', 'gcpindexer.log']
    
    for log_file in log_files:
        file_path = os.path.join(root_dir, log_file)
        result['log_files'][log_file] = {
            'exists': os.path.exists(file_path),
            'size': os.path.getsize(file_path) if os.path.exists(file_path) else 0,
            'sample_lines': []
        }
        
        # Get sample lines from log file
        if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
            try:
                with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                    lines = f.readlines()
                    # Get up to 20 sample lines
                    result['log_files'][log_file]['sample_lines'] = [line.strip() for line in lines[:20]]
            except Exception as e:
                result['log_files'][log_file]['error'] = str(e)
    
    # Add sample logs to help with troubleshooting
    sample_logs = []
    
    # Add master log sample
    sample_logs.append({
        'type': 'master',
        'timestamp': time.time(),
        'message': '2025-05-15 18:29:40,766 - Master - INFO - GCP Master initialized with Google Cloud services'
    })
    
    # Add crawler log sample
    sample_logs.append({
        'type': 'crawler',
        'timestamp': time.time(),
        'message': '2025-05-15 18:29:47,022 - Crawler - INFO - Created subscription: projects/distributed-crawler-456413/subscriptions/web-crawler-crawler-04d7d135'
    })
    
    # Add indexer log sample
    sample_logs.append({
        'type': 'indexer',
        'timestamp': time.time(),
        'message': '2025-05-15 18:29:48,314 - Indexer - INFO - Created subscription: projects/distributed-crawler-456413/subscriptions/web-crawler-indexer-7c98bd63'
    })
    
    result['sample_logs'] = sample_logs
    
    return jsonify(result)

@app.route('/retry_failed', methods=['POST'])
def retry_failed():
    """Retry URLs that previously failed"""
    # Check if the client wants HTML or JSON (default to JSON for API compatibility)
    wants_html = request.args.get('html', 'false').lower() == 'true'
    # Always treat direct browser form submission as wanting HTML
    if request.content_type and 'application/x-www-form-urlencoded' in request.content_type:
        wants_html = True

    try:
        # Read the current crawl state
        root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        state_file = os.path.join(root_dir, 'data', 'state', 'current_state.json')
        
        if not os.path.exists(state_file):
            message = 'No crawl state found. Start a crawl first.'
            if wants_html:
                return get_start_crawl_error_html(message)
            else:
                return jsonify({
                    'status': 'error',
                    'message': message
                })
        
        # Load the state
        with open(state_file, 'r') as f:
            state = json.load(f)
        
        # Get failed URLs
        failed_urls = state.get('failed', [])
        
        if not failed_urls:
            message = 'No failed URLs to retry.'
            if wants_html:
                return get_start_crawl_success_html(message)
            else:
                return jsonify({
                    'status': 'success',
                    'message': message,
                    'retried_count': 0
                })
        
        # Move failed URLs back to the queue
        queue = state.get('queue', [])
        
        # Add each failed URL to the queue
        for url in failed_urls:
            if url not in queue:
                queue.append(url)
        
        # Clear the failed list
        state['failed'] = []
        state['queue'] = queue
        
        # Update the state file
        with open(state_file, 'w') as f:
            json.dump(state, f, indent=2)
            
        # Clear problematic domains cache by creating a file that the crawler will check
        try:
            cache_reset_file = os.path.join(root_dir, 'clear_problematic_domains.flag')
            with open(cache_reset_file, 'w') as f:
                f.write(f"Reset requested at {datetime.now().isoformat()}")
            logging.info(f"Created flag file to clear problematic domains cache: {cache_reset_file}")
        except Exception as e:
            logging.error(f"Failed to create problematic domains reset flag: {e}")
        
        message = f'Successfully moved {len(failed_urls)} failed URLs back to the queue for retry.'
        if wants_html:
            return get_start_crawl_success_html(message)
        else:
            return jsonify({
                'status': 'success',
                'message': message,
                'retried_count': len(failed_urls)
            })
            
    except Exception as e:
        message = f'Error retrying failed URLs: {str(e)}'
        if wants_html:
            return get_start_crawl_error_html(message)
        else:
            return jsonify({
                'status': 'error',
                'message': message
            })

if __name__ == '__main__':
    # Change the working directory to the project root
    root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    os.chdir(root_dir)
    print(f"Working directory set to: {os.getcwd()}")
    
    # Check Python environment
    print(f"Python executable: {sys.executable}")
    print(f"Python version: {sys.version}")
    
    # Check if critical files exist
    script_path = os.path.join(root_dir, 'src', 'run_gcloud_crawler.py')
    if os.path.exists(script_path):
        print(f"Found crawler script at: {script_path}")
        
        # Try to import key modules that might be needed
        try:
            import google.cloud
            print("Google Cloud SDK is available")
        except ImportError:
            print("WARNING: Google Cloud SDK not found. Cloud mode may not work.")
            
        try:
            from bs4 import BeautifulSoup
            print("BeautifulSoup is available")
        except ImportError:
            print("WARNING: BeautifulSoup not found. Crawler may not work properly.")
            
    else:
        print(f"WARNING: Crawler script not found at: {script_path}")
        
    # Ensure data directories exist
    os.makedirs(os.path.join(root_dir, 'data', 'html'), exist_ok=True)
    os.makedirs(os.path.join(root_dir, 'data', 'search_requests'), exist_ok=True)
    os.makedirs(os.path.join(root_dir, 'data', 'search_results'), exist_ok=True)
    
    # Run the Flask app
    port = int(os.environ.get("PORT", 8080))
    app.run(debug=True, host="0.0.0.0", port=port)
