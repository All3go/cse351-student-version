"""
Course: CSE 351
Assignment: 06
Author: [Your Name]

Instructions:

- see instructions in the assignment description in Canvas

""" 

import multiprocessing as mp
import os
import cv2
import numpy as np

from cse351 import *

# Folders
INPUT_FOLDER = "faces"
STEP1_OUTPUT_FOLDER = "step1_smoothed"
STEP2_OUTPUT_FOLDER = "step2_grayscale"
STEP3_OUTPUT_FOLDER = "step3_edges"

# Parameters for image processing
GAUSSIAN_BLUR_KERNEL_SIZE = (5, 5)
CANNY_THRESHOLD1 = 75
CANNY_THRESHOLD2 = 155

# Allowed image extensions
ALLOWED_EXTENSIONS = ['.jpg']

# ---------------------------------------------------------------------------
def create_folder_if_not_exists(folder_path):
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
        print(f"Created folder: {folder_path}")

# ---------------------------------------------------------------------------
def task_convert_to_grayscale(image):
    if len(image.shape) == 2 or (len(image.shape) == 3 and image.shape[2] == 1):
        return image # Already grayscale
    return cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)

# ---------------------------------------------------------------------------
def task_smooth_image(image, kernel_size):
    return cv2.GaussianBlur(image, kernel_size, 0)

# ---------------------------------------------------------------------------
def task_detect_edges(image, threshold1, threshold2):
    if len(image.shape) == 3 and image.shape[2] == 3:
        print("Warning: Applying Canny to a 3-channel image. Converting to grayscale first for Canny.")
        image = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    elif len(image.shape) == 3 and image.shape[2] != 1 : # Should not happen with typical images
        print(f"Warning: Input image for Canny has an unexpected number of channels: {image.shape[2]}")
        return image # Or raise error
    return cv2.Canny(image, threshold1, threshold2)

# ---------------------------------------------------------------------------
def worker(input_queue):
    while True:
        filename = input_queue.get()
        if filename is None:
            break
        try:
            path_in = os.path.join(INPUT_FOLDER, filename)
            img = cv2.imread(path_in)
            if img is None:
                continue
            img = task_smooth_image(img, GAUSSIAN_BLUR_KERNEL_SIZE)
            img = task_convert_to_grayscale(img)
            img = task_detect_edges(img, CANNY_THRESHOLD1, CANNY_THRESHOLD2)
            path_out = os.path.join(STEP3_OUTPUT_FOLDER, filename)
            cv2.imwrite(path_out, img)
        except Exception as e:
            print(f"Error processing {filename}: {e}")

def run_image_processing_pipeline():
    print("Starting parallel image processing pipeline...")
    create_folder_if_not_exists(STEP3_OUTPUT_FOLDER)
    queue = mp.Queue()

    for filename in os.listdir(INPUT_FOLDER):
        if filename.lower().endswith('.jpg'):
            queue.put(filename)

    num_workers = mp.cpu_count()
    workers = []
    for _ in range(num_workers):
        p = mp.Process(target=worker, args=(queue,))
        p.start()
        workers.append(p)

    for _ in range(num_workers):
        queue.put(None)

    for p in workers:
        p.join()

    print("Finished! All edge-detected images saved to 'step3_edges'.")

if __name__ == "__main__":
    log = Log(show_terminal=True)
    log.start_timer('Processing Images')

    if not os.path.isdir(INPUT_FOLDER):
        print(f"Error: The input folder '{INPUT_FOLDER}' was not found.")
        print('Link to faces.zip:')
        print('   https://drive.google.com/file/d/1eebhLE51axpLZoU6s_Shtw1QNcXqtyHM/view?usp=sharing')
    else:
        run_image_processing_pipeline()

    log.write()
    log.stop_timer('Total Time To complete')