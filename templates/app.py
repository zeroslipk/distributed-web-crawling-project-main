from flask import Flask, render_template, request, redirect, url_for
import subprocess

app = Flask(__name__)

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/crawl', methods=['POST'])
def crawl():
    url = request.form['url']
    if url:
        # Run the master.py script to start the crawling process
        # You can pass the URL to the Master node through an API or environment variable
        subprocess.Popen(["mpiexec", "-n", "3", "python3", "master.py", url])
        return redirect(url_for('home'))
    return "Please provide a valid URL"

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)