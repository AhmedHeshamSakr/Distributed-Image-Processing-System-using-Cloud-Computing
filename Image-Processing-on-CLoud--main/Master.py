import socket
import threading
import logging
import requests
import atexit
import time
import signal
from azure.storage.queue import QueueServiceClient

# Define server address and port
server_address = '0.0.0.0'  # Listen on all available interfaces
server_port = 5000

# Azure Queue Storage settings
connection_string = "_____________"
queue_name = 'taskqueue'
queue_service_client = QueueServiceClient.from_connection_string(connection_string)
task_queue_client = queue_service_client.get_queue_client(queue_name)

flask_server_url = 'http://localhost:5001/status'
flask_add_result_url = 'http://localhost:5001/add_result'
flask_clear_all_url = 'http://localhost:5001/clear_all'

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# Suppress Azure SDK debug logs
logging.getLogger('azure.core.pipeline.policies.http_logging_policy').setLevel(logging.WARNING)

# Task queue and results
results_lock = threading.Lock()
results = []
worker_threads = []
worker_status_lock = threading.Lock()
worker_status = {}
tasks_queue_lock = threading.Lock()
tasks_queue = []
assigned_tasks_lock = threading.Lock()
assigned_tasks = {}  # Track tasks assigned to workers
running = True  # Flag to control the main loop

def fetch_tasks_from_azure_queue():
    global task_queue_client, tasks_queue
    try:
        messages = task_queue_client.receive_messages(messages_per_page=32)
        fetched_tasks = 0
        for msg in messages:
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

def send_status_update(worker_id, status):
    try:
        response = requests.post(flask_server_url, json={'worker_id': worker_id, 'status': status})
        if response.status_code != 200:
            logging.error(f"Failed to send status update: {response.content}")
    except Exception as e:
        logging.error(f"Failed to send status update: {e}")

def add_result(result):
    try:
        response = requests.post(flask_add_result_url, json={'result': result})
        if response.status_code != 200:
            logging.error(f"Failed to add result: {response.content}")
    except Exception as e:
        logging.error(f"Failed to add result: {e}")

def clear_all():
    try:
        response = requests.post(flask_clear_all_url)
        if response.status_code == 200:
            logging.info("Cleared all statuses and results from Flask server")
        else:
            logging.error(f"Failed to clear all statuses and results from Flask server: {response.content}")
    except Exception as e:
        logging.error(f"Failed to clear all statuses and results from Flask server: {e}")

def handle_worker_connection(worker_socket, address):
    worker_id = f"{address[0]}:{address[1]}"
    logging.info(f"Worker {worker_id} connected")
    with worker_status_lock:
        worker_status[worker_id] = 'connected'
    send_status_update(worker_id, 'connected')

    try:
        while running:
            logging.info("Checking for tasks in queue...")
            with tasks_queue_lock:
                if tasks_queue:
                    task = tasks_queue.pop(0)  # Get the first task from the queue
                else:
                    task = None
            if task:
                task_message = ",".join(task)
                with assigned_tasks_lock:
                    assigned_tasks[worker_id] = task  # Track the assigned task
                with worker_status_lock:
                    worker_status[worker_id] = f'processing task {task_message}'
                send_status_update(worker_id, f'processing task {task_message}')
                worker_socket.sendall(task_message.encode())  # Send task to worker
                try:
                    logging.info(f"Sent task to worker {worker_id}: {task_message}")
                    result = worker_socket.recv(4096).decode()  # Increase buffer size for larger results
                    logging.info(f"Received result from worker {worker_id}: {result}")
                    if result:
                        with results_lock:
                            results.append(result)
                        logging.info(f"Task {task[0]}, {task[1]} completed by worker {worker_id} with result {result}")
                        add_result(result)  # Add result to Flask server
                        with assigned_tasks_lock:
                            assigned_tasks.pop(worker_id, None)  # Remove from assigned tasks
                    else:
                        logging.error(f"Received empty result for task {task_message} from worker {worker_id}")
                        raise ValueError("Empty result received")
                except Exception as e:
                    logging.error(f"Error receiving result from worker {worker_id}: {e}")
                    if worker_id in assigned_tasks:
                        with assigned_tasks_lock:
                            tasks_queue.insert(0, assigned_tasks.pop(worker_id))  # Reassign the task
                        logging.info(f"Reassigned task {task_message} due to worker error or empty result")
                    break  # Exit the loop to reconnect the worker
            else:
                logging.info("No tasks in queue, sending NO_TASK signal to worker")
                worker_socket.sendall("NO_TASK".encode())  # Send "no task" signal to the worker
                time.sleep(5)  # Wait before checking the queue again
    except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError, ValueError) as e:
        logging.error(f"Connection to worker {worker_id} lost or task error: {e}")
        if worker_id in assigned_tasks:
            with assigned_tasks_lock:
                tasks_queue.insert(0, assigned_tasks.pop(worker_id))  # Reassign the task
            logging.info(f"Reassigned task {task_message} due to worker disconnection or task error")
        send_status_update(worker_id, 'disconnected')
    except Exception as e:
        logging.error(f"Error with worker {worker_id}: {e}")
    finally:
        worker_socket.close()
        logging.info(f"Worker {worker_id} disconnected")
        with worker_status_lock:
            worker_status[worker_id] = 'disconnected'
        send_status_update(worker_id, 'disconnected')

def accept_connections():
    while running:
        try:
            worker_socket, worker_address = server_socket.accept()
            logging.info(f"Accepted connection from {worker_address}")
            worker_thread = threading.Thread(target=handle_worker_connection, args=(worker_socket, worker_address))
            worker_thread.start()
            worker_threads.append(worker_thread)
        except Exception as e:
            logging.error(f"Error accepting connections: {e}")
            break

def cleanup():
    global running
    running = False
    logging.info("Shutting down server...")
    server_socket.close()
    logging.info("Server socket closed.")
    # Ensure all worker threads have completed
    for thread in worker_threads:
        thread.join()
    logging.info("All tasks have been assigned and results collected.")
    logging.info(f"Results: {results}")
    clear_all()  # Clear all statuses and results from Flask server

# Register the cleanup function
atexit.register(cleanup)

# Signal handler for graceful shutdown
def signal_handler(sig, frame):
    logging.info("Interrupt received, shutting down...")
    cleanup()
    exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# Set up server
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.bind((server_address, server_port))
server_socket.listen(5)  # Listen for incoming connections

logging.info(f"Master node is listening for connections on port {server_port}...")

# Start accepting connections in a separate thread
accept_thread = threading.Thread(target=accept_connections)
accept_thread.start()

def continuous_task_fetch():
    while running:
        tasks_fetched = fetch_tasks_from_azure_queue()
        if tasks_fetched > 0:
            logging.info("New tasks fetched and queued")
        time.sleep(10)  # Check for new tasks every 10 seconds

# Start continuously fetching tasks in a separate thread
task_fetch_thread = threading.Thread(target=continuous_task_fetch)
task_fetch_thread.start()

try:
    while accept_thread.is_alive():
        accept_thread.join(1)
except KeyboardInterrupt:
    logging.info("Interrupt received, shutting down...")
finally:
    cleanup()  # Ensure cleanup is called if the script is interrupted
