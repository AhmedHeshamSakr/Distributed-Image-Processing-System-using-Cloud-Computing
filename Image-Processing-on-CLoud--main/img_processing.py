import sys
import cv2
import numpy as np
import logging
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import os
import time

# Setup basic configuration for logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize Azure Blob Service Client
connection_string = "DefaultEndpointsProtocol=https;AccountName=vm123store;AccountKey=LhRPPjDK4YLM6J0v4YABGxKv9vZoJW91+UcDNR+MOgQ33EYHJxJjrM76UkqhULe72/yAa8V/AkwG+AStgHuf3g==;EndpointSuffix=core.windows.net"

blob_service_client = BlobServiceClient.from_connection_string(connection_string)
image_container_client = blob_service_client.get_container_client("myone")  # Container for images
result_container_client = blob_service_client.get_container_client("myresult")  # Container for res>

def download_from_azure(blob_name, download_path):
    blob_client = image_container_client.get_blob_client(blob=blob_name)
    try:
        with open(download_path, "wb") as download_file:
            download_file.write(blob_client.download_blob().readall())
        logging.info(f"Downloaded {blob_name} from Azure Blob Storage to {download_path}.")
    except Exception as e:
        logging.error(f"Failed to download {blob_name} from Azure Blob Storage: {e}")

def upload_to_azure(file_path, file_name, retries=3):
    blob_client = result_container_client.get_blob_client(blob=file_name)
    attempt = 0
    while attempt < retries:
        try:
            with open(file_path, "rb") as data:
                blob_client.upload_blob(data, overwrite=True)
            logging.info(f"Uploaded {file_name} to Azure Blob Storage.")
            os.remove(file_path)
            logging.info(f"Local file {file_path} deleted after successful upload.")
            return blob_client.url  # Return the URL of the uploaded blob
        except Exception as e:
            logging.error(f"Failed to upload {file_name} to Azure Blob Storage (attempt {attempt+1}): {e}")
            attempt += 1
            time.sleep(5)  # Wait before retrying
    logging.error(f"Failed to upload {file_name} to Azure Blob Storage after {retries} attempts.")
    return None

def save_image(image, base_name):
    local_path = "./"
    unique_suffix = time.strftime("%Y%m%d-%H%M%S")
    file_name = f"{base_name}_{unique_suffix}.jpg"
    file_path = os.path.join(local_path, file_name)
    try:
        cv2.imwrite(file_path, image)
        logging.info(f"Image saved locally as {file_path}")
        result_url = upload_to_azure(file_path, file_name)
        if result_url:
            logging.info(f"Result uploaded to Azure Blob Storage at {result_url}")
            return result_url
        else:
            logging.error(f"Failed to upload {file_name} to Azure Blob Storage.")
            logging.error(f"Failed to upload {file_name} to Azure Blob Storage.")
            return None
    except Exception as e:
        logging.error(f"Failed to save and upload image {file_name}: {e}")
        return None

# Functions for image processing
def watershed_segmentation(image_path):
    logging.info(f"Starting watershed segmentation on {image_path}")
    image = cv2.imread(image_path)
    if image is None:
        logging.error(f"Failed to load image at {image_path}")
        return None
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    _, thresh = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)
    kernel = np.ones((3, 3), np.uint8)
    opening = cv2.morphologyEx(thresh, cv2.MORPH_OPEN, kernel, iterations=2)
    sure_bg = cv2.dilate(opening, kernel, iterations=3)
    dist_transform = cv2.distanceTransform(opening, cv2.DIST_L2, 5)
    _, sure_fg = cv2.threshold(dist_transform, 0.7 * dist_transform.max(), 255, 0)
    sure_fg = np.uint8(sure_fg)
    unknown = cv2.subtract(sure_bg, sure_fg)
    _, markers = cv2.connectedComponents(sure_fg)
    markers = markers + 1
    markers[unknown == 255] = 0
    markers = cv2.watershed(image, markers)
    image[markers == -1] = [255, 0, 0]
    logging.info(f"Completed watershed segmentation on {image_path}")
    return save_image(image, "watershed_segmented")

def canny_edge_detector(image_path):
    logging.info(f"Starting Canny edge detection on {image_path}")
    image = cv2.imread(image_path, 0)
    if image is None:
        logging.error(f"Failed to load image at {image_path}")
        return None
    edges = cv2.Canny(image, 100, 200)
    logging.info(f"Completed Canny edge detection on {image_path}")
    return save_image(edges, "canny_edges")

def feature_matching(image_path1, image_path2):
    logging.info(f"Starting feature matching between {image_path1} and {image_path2}")
    img1 = cv2.imread(image_path1, cv2.IMREAD_GRAYSCALE)
    img2 = cv2.imread(image_path2, cv2.IMREAD_GRAYSCALE)
    if img1 is None or img2 is None:
        logging.error(f"Failed to load images at {image_path1} or {image_path2}")
        return None
    orb = cv2.ORB_create()
    kp1, des1 = orb.detectAndCompute(img1, None)
    kp2, des2 = orb.detectAndCompute(img2, None)
    bf = cv2.BFMatcher(cv2.NORM_HAMMING, crossCheck=True)
    matches = bf.match(des1, des2)
    matches = sorted(matches, key=lambda x: x.distance)
    img3 = cv2.drawMatches(img1, kp1, img2, kp2, matches[:10], None, flags=2)
    logging.info(f"Completed feature matching between {image_path1} and {image_path2}")
    return save_image(img3, "feature_matches")

def face_detection(image_path):
    logging.info(f"Starting face detection on {image_path}")
    img = cv2.imread(image_path)
    if img is None:
        logging.error(f"Failed to load image at {image_path}")
        return None
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    face_cascade = cv2.CascadeClassifier(cv2.data.haarcascades + 'haarcascade_frontalface_default.xml')
    faces = face_cascade.detectMultiScale(gray, 1.3, 5)
    for (x, y, w, h) in faces:
        cv2.rectangle(img, (x, y), (x+w, y+h), (255, 0, 0), 2)
    logging.info(f"Completed face detection on {image_path}")
    return save_image(img, "detected_faces")

if __name__ == "__main__":
    operation = sys.argv[1]
    if operation == "feature_matching":
        image_path1 = sys.argv[2]
        image_path2 = sys.argv[3]
        # Download images from Azure Blob Storage
        download_from_azure(image_path1, image_path1)
        download_from_azure(image_path2, image_path2)
        result_path = feature_matching(image_path1, image_path2)
    else:
        image_path = sys.argv[2]
        # Download image from Azure Blob Storage
        download_from_azure(image_path, image_path)
        if operation == "watershed_segmentation":
            result_path = watershed_segmentation(image_path)
        elif operation == "canny_edge_detector":
            result_path = canny_edge_detector(image_path)
        elif operation == "face_detection":
            result_path = face_detection(image_path)
        else:
            logging.error("Invalid operation")
            sys.exit(1)
    if result_path:
        print(result_path)
    else:
        logging.error("Operation failed")
        sys.exit(1)

