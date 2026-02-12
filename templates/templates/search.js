// Enhanced search functionality for the web crawler interface
document.addEventListener('DOMContentLoaded', function() {
    const searchForm = document.getElementById('searchForm');
    const searchInput = document.getElementById('searchInput');
    const searchButton = document.getElementById('searchButton');
    const searchResults = document.getElementById('searchResults');
    const urlCheckForm = document.getElementById('urlCheckForm');
    const urlInput = document.getElementById('urlInput');
    const checkUrlButton = document.getElementById('checkUrlButton');

    // Add loading state to buttons
    function setLoading(button, isLoading) {
        if (isLoading) {
            button.setAttribute('disabled', 'disabled');
            button.innerHTML = '<span class="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span> Loading...';
        } else {
            button.removeAttribute('disabled');
            button.innerHTML = button.getAttribute('data-original-text');
        }
    }

    // Store original button text
    if (searchButton) {
        searchButton.setAttribute('data-original-text', searchButton.innerHTML);
    }
    if (checkUrlButton) {
        checkUrlButton.setAttribute('data-original-text', checkUrlButton.innerHTML);
    }

    // Handle search
    if (searchForm) {
        searchForm.addEventListener('submit', function(e) {
            e.preventDefault();
            
            const query = searchInput.value.trim();
            const field = document.getElementById('fieldSelect').value;
            
            if (!query) {
                showAlert('Please enter a search term');
                return;
            }
            
            setLoading(searchButton, true);
            searchResults.innerHTML = '<div class="text-center my-4"><div class="spinner-border text-primary" role="status"></div><p class="mt-2">Searching...</p></div>';
            
            // Set a timeout to handle long-running searches
            const timeoutId = setTimeout(() => {
                // If search takes too long, show a message but keep waiting
                searchResults.innerHTML = '<div class="alert alert-warning" role="alert">Search is taking longer than expected. Still searching...</div>';
            }, 3000);
            
            fetch('/search', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/x-www-form-urlencoded',
                },
                body: new URLSearchParams({
                    'query': query,
                    'field': field
                })
            })
            .then(response => response.json())
            .then(data => {
                clearTimeout(timeoutId);
                setLoading(searchButton, false);
                
                if (data.error) {
                    searchResults.innerHTML = `<div class="alert alert-danger" role="alert">${data.error}</div>`;
                    return;
                }
                
                if (!data.length) {
                    searchResults.innerHTML = `<div class="alert alert-info" role="alert">No results found for "${query}" in ${field}</div>`;
                    return;
                }
                
                // Display results
                let html = `<h3 class="mt-4">Search Results for "${query}"</h3>`;
                html += '<div class="list-group">';
                
                data.forEach(result => {
                    const url = result.url;
                    const title = result.title || url;
                    const snippet = result.snippet || 'No preview available';
                    
                    html += `
                        <div class="list-group-item">
                            <div class="d-flex w-100 justify-content-between">
                                <h5 class="mb-1">
                                    <a href="${url}" target="_blank">${title}</a>
                                </h5>
                                <small>Score: ${result.score}</small>
                            </div>
                            <p class="mb-1">${snippet}</p>
                            <small>${result.timestamp || 'Unknown date'}</small>
                            <div class="mt-2">
                                <button class="btn btn-sm btn-outline-primary view-html-btn" data-url="${url}">View HTML</button>
                            </div>
                        </div>
                    `;
                });
                
                html += '</div>';
                searchResults.innerHTML = html;
                
                // Add event listeners for View HTML buttons
                document.querySelectorAll('.view-html-btn').forEach(button => {
                    button.addEventListener('click', function() {
                        viewHtmlContent(this.getAttribute('data-url'));
                    });
                });
            })
            .catch(error => {
                clearTimeout(timeoutId);
                setLoading(searchButton, false);
                searchResults.innerHTML = `<div class="alert alert-danger" role="alert">Search failed: ${error.message}</div>`;
                console.error('Search error:', error);
            });
        });
    }
    
    // Handle URL check
    if (urlCheckForm) {
        urlCheckForm.addEventListener('submit', function(e) {
            e.preventDefault();
            
            const url = urlInput.value.trim();
            
            if (!url) {
                showAlert('Please enter a URL to check');
                return;
            }
            
            if (!url.startsWith('http://') && !url.startsWith('https://')) {
                showAlert('Please enter a valid URL starting with http:// or https://');
                return;
            }
            
            setLoading(checkUrlButton, true);
            
            fetch('/check_url', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/x-www-form-urlencoded',
                },
                body: new URLSearchParams({
                    'url': url
                })
            })
            .then(response => response.json())
            .then(data => {
                setLoading(checkUrlButton, false);
                
                if (data.error) {
                    showAlert(data.error, 'danger');
                    return;
                }
                
                if (data.crawled === true) {
                    showAlert(`URL has been crawled. ${data.message}`, 'success');
                    
                    // Add a button to view the HTML content
                    searchResults.innerHTML += `
                        <div class="mt-3">
                            <button class="btn btn-primary view-html-btn" data-url="${url}">View Content</button>
                        </div>
                    `;
                    
                    // Add event listener for the View Content button
                    document.querySelector('.view-html-btn').addEventListener('click', function() {
                        viewHtmlContent(url);
                    });
                } else if (data.crawled === 'in_progress') {
                    showAlert(`${data.message}`, 'warning');
                } else {
                    showAlert(`${data.message}`, 'danger');
                }
            })
            .catch(error => {
                setLoading(checkUrlButton, false);
                showAlert(`Failed to check URL: ${error.message}`, 'danger');
                console.error('URL check error:', error);
            });
        });
    }
    
    // Function to view HTML content
    function viewHtmlContent(url) {
        const modalId = 'htmlContentModal';
        let modal = document.getElementById(modalId);
        
        // Create modal if it doesn't exist
        if (!modal) {
            const modalHtml = `
                <div class="modal fade" id="${modalId}" tabindex="-1" aria-labelledby="${modalId}Label" aria-hidden="true">
                    <div class="modal-dialog modal-xl">
                        <div class="modal-content">
                            <div class="modal-header">
                                <h5 class="modal-title" id="${modalId}Label">HTML Content</h5>
                                <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
                            </div>
                            <div class="modal-body">
                                <div id="htmlContentLoading" class="text-center">
                                    <div class="spinner-border text-primary" role="status"></div>
                                    <p>Loading content...</p>
                                </div>
                                <div id="htmlContentError" class="alert alert-danger d-none"></div>
                                <div id="htmlContentDisplay" class="d-none">
                                    <ul class="nav nav-tabs" id="contentTabs" role="tablist">
                                        <li class="nav-item" role="presentation">
                                            <button class="nav-link active" id="rendered-tab" data-bs-toggle="tab" data-bs-target="#rendered" type="button" role="tab">Rendered</button>
                                        </li>
                                        <li class="nav-item" role="presentation">
                                            <button class="nav-link" id="raw-tab" data-bs-toggle="tab" data-bs-target="#raw" type="button" role="tab">Raw HTML</button>
                                        </li>
                                    </ul>
                                    <div class="tab-content mt-3" id="contentTabsContent">
                                        <div class="tab-pane fade show active" id="rendered" role="tabpanel">
                                            <iframe id="htmlContentFrame" style="width: 100%; height: 600px; border: 1px solid #ddd;"></iframe>
                                        </div>
                                        <div class="tab-pane fade" id="raw" role="tabpanel">
                                            <pre id="htmlContentRaw" style="max-height: 600px; overflow: auto;"></pre>
                                        </div>
                                    </div>
                                </div>
                            </div>
                            <div class="modal-footer">
                                <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Close</button>
                            </div>
                        </div>
                    </div>
                </div>
            `;
            
            document.body.insertAdjacentHTML('beforeend', modalHtml);
            modal = document.getElementById(modalId);
        }
        
        // Reset modal state
        document.getElementById('htmlContentLoading').classList.remove('d-none');
        document.getElementById('htmlContentError').classList.add('d-none');
        document.getElementById('htmlContentDisplay').classList.add('d-none');
        document.getElementById('htmlContentRaw').textContent = '';
        document.getElementById('htmlContentFrame').srcdoc = '';
        
        // Show modal
        const bsModal = new bootstrap.Modal(modal);
        bsModal.show();
        
        // Fetch HTML content
        fetch('/get_html', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/x-www-form-urlencoded',
            },
            body: new URLSearchParams({
                'url': url
            })
        })
        .then(response => response.json())
        .then(data => {
            document.getElementById('htmlContentLoading').classList.add('d-none');
            
            if (data.status === 'error') {
                document.getElementById('htmlContentError').textContent = data.message;
                document.getElementById('htmlContentError').classList.remove('d-none');
                return;
            }
            
            document.getElementById('htmlContentDisplay').classList.remove('d-none');
            document.getElementById('htmlContentRaw').textContent = data.html;
            
            // Escape HTML content for the iframe
            const htmlContent = data.html;
            
            // Set iframe content
            const iframe = document.getElementById('htmlContentFrame');
            iframe.srcdoc = htmlContent;
            
            // Update modal title
            document.getElementById(`${modalId}Label`).textContent = `Content for: ${url}`;
        })
        .catch(error => {
            document.getElementById('htmlContentLoading').classList.add('d-none');
            document.getElementById('htmlContentError').textContent = `Failed to load content: ${error.message}`;
            document.getElementById('htmlContentError').classList.remove('d-none');
            console.error('HTML content error:', error);
        });
    }
    
    // Helper function to show alerts
    function showAlert(message, type = 'info') {
        const alert = document.createElement('div');
        alert.className = `alert alert-${type} alert-dismissible fade show`;
        alert.innerHTML = `
            ${message}
            <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
        `;
        
        searchResults.innerHTML = '';
        searchResults.appendChild(alert);
    }
}); 