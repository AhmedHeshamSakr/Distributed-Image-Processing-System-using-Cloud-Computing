
# Distributed Image Processing System

## Introduction

This project presents a distributed image processing system leveraging cloud computing technologies. The system is designed to efficiently handle large-scale image processing tasks by distributing workloads across multiple virtual machines (VMs) in a cloud environment. The key objectives include scalability, fault tolerance, and efficient processing, making it a robust platform for various applications in medical imaging, computer vision, and entertainment.

## Key Features

- **Scalability**: The system can dynamically scale by adding more VMs as the workload increases, maintaining processing speed and efficiency.
- **Fault Tolerance**: The system ensures reliability by redistributing tasks in the event of node failures, ensuring continuous operation.
- **Efficiency**: Leveraging parallel processing techniques, the system performs complex image processing tasks quickly and efficiently.
- **User Interface**: A Flask-based web interface allows users to upload images, select processing operations, monitor progress, and download processed images.

## Technologies Used

- **Python**: Main programming language, chosen for its simplicity and extensive library support.
- **OpenCV**: Used for image processing operations like filtering, edge detection, and color manipulation.
- **Microsoft Azure**: Cloud platform used for provisioning VMs, storage, and task management.
- **Flask**: Provides the web interface for user interaction.

## System Architecture

- **Master Node**: Manages tasks and worker nodes using Azure Queue Storage for task distribution and Azure Blob Storage for image and result storage. It also runs the Flask server.
- **Worker Nodes**: Fetch tasks from Azure Queue Storage, process images using OpenCV, and upload results back to Azure Blob Storage.
- **Flask Application**: Allows users to upload images, specify processing operations, and view/download results.

### Process Flow

1. **File Upload & Task Creation**: Users upload images through the Flask interface, which are then saved locally and uploaded to Azure Blob Storage. A task message is added to Azure Queue Storage.
2. **Task Distribution**: Worker nodes fetch tasks from Azure Queue Storage and process the corresponding images.
3. **Image Processing**: Using OpenCV, worker nodes perform the specified processing operations and upload results to Azure Blob Storage.
4. **Result Retrieval**: Users can monitor task status and download processed images through the Flask interface.

## Image Processing Operations

- **Watershed Segmentation**
- **Canny Edge Detection**
- **Feature Matching**
- **Face Detection**

## Scalability and Fault Tolerance

- **Scalability**: The system scales using Azure's cloud-based storage and queuing, allowing dynamic distribution of tasks. Worker nodes can be added or removed based on workload.
- **Fault Tolerance**: The system handles network issues and node failures by reassigning tasks to other available nodes and logging critical events for monitoring.

## Testing

The system underwent extensive testing for functionality, performance, scalability, and fault tolerance. Key results include:

- Successful file uploads and accurate processing operations.
- Efficient task handling with acceptable response times.
- Dynamic scaling based on workload.
- Robust fault tolerance with task reassignment and system resilience.

## Deployment

The system was deployed to the cloud using Azure services:

1. **Azure Resource Setup**: Blob Storage for image storage and Queue Storage for task messages.
2. **VM Configuration**: Setup of master and worker VMs, including environment configuration and dependency installation.
3. **Code Deployment**: Deployment of scripts to respective VMs and starting the Flask server and worker processes.
4. **Monitoring and Scaling**: Initial log monitoring and dynamic scaling of worker nodes based on workload.

## How to Set Up

### Prerequisites

- Python 3.x
- Microsoft Azure Account
- Flask
- OpenCV


## Documentation

- **System Architecture**: Detailed description of components and data flow.
- **Setup and Configuration**: Instructions for setting up the development environment and configuration files.
- **Deployment Guide**: Step-by-step instructions for deploying the system to Azure.
- **Testing Overview**: Summary of testing procedures and results.

## Conclusion

This project successfully demonstrates the use of cloud-based distributed computing for image processing tasks. The system's scalable architecture, fault tolerance, and advanced processing capabilities make it a versatile and robust platform for various image processing applications.




