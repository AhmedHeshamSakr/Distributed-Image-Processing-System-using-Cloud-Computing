import socket
import subprocess
import logging
import time

# Define master address and port
master_address = '20.163.175.53'  # Master VM IP address
master_port = 5000

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def connect_to_master():
    while True:
        try:
            worker_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            worker_socket.connect((master_address, master_port))
            logging.info(f"Connected to master at {master_address}:{master_port}")
            return worker_socket
        except Exception as e:
            logging.error(f"Connection failed: {e}")
            logging.info("Retrying in 5 seconds...")
            time.sleep(5)

def execute_task(task_args):
    try:
        if task_args[1] == "feature_matching":
            cmd = ["python3", "img_processing.py", task_args[1], task_args[0], task_args[2]]
        else:
            cmd = ["python3", "img_processing.py", task_args[1], task_args[0]]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            return result.stdout.strip()
        else:
            logging.error(f"Task {task_args} failed: {result.stderr}")
            return "ERROR"
    except Exception as e:
        logging.error(f"Error executing task {task_args}: {e}")
        return "ERROR"

def main():
    while True:
        worker_socket = connect_to_master()
        try:
            while True:
                task_data = worker_socket.recv(1024).decode()
                if not task_data:
                    time.sleep(1)  # Wait a bit before checking for new tasks
                    continue
                if task_data == "NO_TASK":
                    logging.info("No tasks available, waiting for new tasks...")
                    time.sleep(5)  # Wait before checking again
                    continue
                logging.info(f"Received task: {task_data}")
                task_args = task_data.split(',')
                result = execute_task(task_args)
                worker_socket.sendall(result.encode())
                logging.info(f"Task {task_data} completed with result: {result}")
        except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError) as e:
            logging.error(f"Connection to master lost: {e}")
            logging.info("Reconnecting to master...")
            worker_socket.close()
            break
        except Exception as e:
            logging.error(f"Error during task processing: {e}")
        finally:
            worker_socket.close()
            logging.info("Worker disconnected")

if __name__ == "__main__":
    while True:
        main()

