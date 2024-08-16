from flask import Flask, request, render_template, jsonify, redirect, url_for
import os
import threading
import time
import signal
from azure.storage.blob import BlobServiceClient
from azure.storage.queue import QueueServiceClient
from werkzeug.utils import secure_filename
import logging

app = Flask(__name__)
UPLOAD_FOLDER = 'uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# Azure Blob Storage details
AZURE_CONNECTION_STRING = " --------- "
IMAGE_CONTAINER_NAME = "myone"
RESULT_CONTAINER_NAME = "myresult"
QUEUE_NAME = 'taskqueue'

# Initialize Azure Blob Service Client
blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
image_container_client = blob_service_client.get_container_client(IMAGE_CONTAINER_NAME)
result_container_client = blob_service_client.get_container_client(RESULT_CONTAINER_NAME)

# Initialize Azure Queue Service Client
queue_service_client = QueueServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
task_queue_client = queue_service_client.get_queue_client(QUEUE_NAME)

# Configure logging for your application
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
# Suppress Azure SDK debug logs
logging.getLogger('azure.core.pipeline.policies.http_logging_policy').setLevel(logging.WARNING)

# Worker statuses
worker_status_lock = threading.Lock()
worker_status = {}
results_lock = threading.Lock()
results = []
tasks_queue_lock = threading.Lock()
tasks_queue = []
stop_event = threading.Event()

@app.route('/')
def index():
    return render_template('index.html', results=results, worker_status=worker_status)

@app.route('/upload', methods=['POST'])
def upload_file():
    files = request.files.getlist('file')
    operation = request.form.get('operation')
    if not files or not operation:
        return 'No file or operation selected', 400
    
    for file in files:
        if file.filename == '':
            return 'No selected file', 400
        filename = secure_filename(file.filename)
        file_path = os.path.join(UPLOAD_FOLDER, filename)
        file.save(file_path)
        upload_url = upload_to_azure(file_path, filename)
        if upload_url:
            task_message = f"{filename},{operation},{upload_url}"
            try:
                task_queue_client.send_message(task_message)
                logging.info(f"Task added to queue: {filename}, {operation}")
            except Exception as e:
                logging.error(f"Failed to add task to queue: {e}")
        else:
            logging.error(f"Failed to upload file to Azure: {filename}")

    return redirect(url_for('index'))

def upload_to_azure(file_path, file_name):
    try:
        blob_client = image_container_client.get_blob_client(file_name)
        with open(file_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)
        os.remove(file_path)
        logging.info(f'{file_name} uploaded to Azure Blob Storage and local file removed.')
        return blob_client.url  # Return the URL of the uploaded blob
    except Exception as e:
        logging.error(f"Failed to upload {file_name} to Azure Blob Storage: {e}")
        return None

@app.route('/status', methods=['POST'])
def update_status():
    data = request.get_json()
    worker_id = data.get('worker_id').split(':')[0]  # Use only the IP address as the key
    status = data.get('status')
    with worker_status_lock:
        if worker_id not in worker_status:
            worker_status[worker_id] = []
        worker_status[worker_id].append(status)
    return jsonify({'message': 'Status updated'}), 200

@app.route('/status', methods=['GET'])
def status():
    with worker_status_lock:
        statuses = [{'id': worker, 'statuses': status_list} for worker, status_list in worker_status.items()]
    return jsonify(statuses)

@app.route('/tasks', methods=['GET'])
def get_tasks():
    with tasks_queue_lock:
        tasks = [message.content for message in task_queue_client.receive_messages(messages_per_page=32)]
    return jsonify(tasks)

@app.route('/clear_tasks', methods=['POST'])
def clear_tasks():
    while True:
        messages = task_queue_client.receive_messages(messages_per_page=32)
        if not messages:
            break
        for message in messages:
            task_queue_client.delete_message(message)
    return jsonify({'message': 'Tasks cleared'}), 200

@app.route('/clear_all', methods=['POST'])
def clear_all():
    global worker_status, results
    with worker_status_lock:
        worker_status = {}
    with results_lock:
        results = []
    clear_tasks()
    return jsonify({'message': 'All statuses and results cleared'}), 200

@app.route('/add_result', methods=['POST'])
def add_result():
    data = request.get_json()
    result = data.get('result')
    if result:
        # Assuming result is in the format "filename,operation,url"
        result_parts = result.split(',')
        if len(result_parts) == 3:
            result_name_operation = f"{result_parts[0]}, {result_parts[1]}"
            with results_lock:
                results.append(result_name_operation)
        else:
            with results_lock:
                results.append(result)
    return jsonify({'message': 'Result added'}), 200

@app.route('/results')
def results_page():
    with results_lock:
        return render_template('results.html', results=results)

def continuous_task_fetch(stop_event):
    while not stop_event.is_set():
        tasks_fetched = fetch_tasks_from_azure_queue()
        if tasks_fetched > 0:
            logging.info("New tasks fetched and queued")
        stop_event.wait(10)  # Delay between fetch attempts

def fetch_tasks_from_azure_queue():
    global task_queue_client, stop_event
    if stop_event.is_set():
        logging.info("Stop event set, stopping fetch_tasks_from_azure_queue")
        return 0
    try:
        messages = task_queue_client.receive_messages(messages_per_page=32)
        fetched_tasks = 0
        for msg in messages:
            if stop_event.is_set():
                logging.info("Stop event set, breaking message processing loop")
                break
            task = msg.content.split(',')
            with tasks_queue_lock:
                tasks_queue.append(task)
            task_queue_client.delete_message(msg)
            fetched_tasks += 1
        if fetched_tasks > 0:
            logging.info(f"Fetched {fetched_tasks} tasks from Azure Queue")
        return fetched_tasks
    except Exception as e:
        logging.error(f"Failed to fetch tasks from Azure Queue: {e}")
        return 0

def update_worker_status(stop_event):
    while not stop_event.is_set():
        stop_event.wait(5)

def signal_handler(sig, frame):
    logging.info("Interrupt received, shutting down...")
    stop_event.set()
    shutdown_server()

def shutdown_server():
    func = request.environ.get('werkzeug.server.shutdown')
    if func is None:
        raise RuntimeError('Not running with the Werkzeug Server')
    func()

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

if __name__ == '__main__':
    status_thread = threading.Thread(target=update_worker_status, args=(stop_event,))
    status_thread.start()

    task_fetch_thread = threading.Thread(target=continuous_task_fetch, args=(stop_event,))
    task_fetch_thread.start()

    try:
        app.run(debug=True, host='0.0.0.0', port=5001)
    except KeyboardInterrupt:
        logging.info("Keyboard interrupt received, shutting down.")
    finally:
        logging.info("Stopping threads...")
        stop_event.set()
        task_fetch_thread.join()
        status_thread.join()
        logging.info("Threads stopped, exiting.")
