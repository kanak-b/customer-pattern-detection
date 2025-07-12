from config import TRANSACTIONS_FILE_ID, TRANSACTIONS_PATH
from utils.gdrive import download_from_gdrive
from utils.chunk_uploader import stream_upload
from utils.stream_detection import consume_stream_detect_patterns

import threading
import time

def main():


    print("Downloading raw transactions file from GDrive...")
    download_from_gdrive(TRANSACTIONS_FILE_ID, TRANSACTIONS_PATH)

    print("Starting streaming pipeline...")

    # Mechanism X – Streaming uploader
    upload_thread = threading.Thread(target=stream_upload, name="UploaderThread", daemon=True)

    # Mechanism Y – Stream consumer & detector
    detection_thread = threading.Thread(target=consume_stream_detect_patterns, name="DetectorThread", daemon=True)

    # Start both threads
    upload_thread.start()
    detection_thread.start()

    # Keep main thread alive while daemon threads run
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down streaming process...")

if __name__ == "__main__":
    main()
