import os
import time
import subprocess
import logging
from datetime import datetime
import requests
from threading import Thread
import signal
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('stream_log.txt'),
        logging.StreamHandler()
    ]
)

class StreamManager:
    def __init__(self):
        self.youtube_stream_key = "cr8p-6zjj-xeux-4cxa-fe5x"
        self.video_paths = [
            "https://hanuman.s3.us-south.cloud-object-storage.appdomain.cloud/0000.mp4",
            "https://hanuman.s3.us-south.cloud-object-storage.appdomain.cloud/0001.mp4",
            "https://hanuman.s3.us-south.cloud-object-storage.appdomain.cloud/0002.mp4",
        ]
        self.current_process = None
        self.is_running = True
        self.setup_signal_handlers()

    def setup_signal_handlers(self):
        signal.signal(signal.SIGINT, self.handle_shutdown)
        signal.signal(signal.SIGTERM, self.handle_shutdown)

    def handle_shutdown(self, signum, frame):
        logging.info("Received shutdown signal. Cleaning up...")
        self.is_running = False
        if self.current_process:
            self.current_process.terminate()
        sys.exit(0)

    def check_internet_connection(self):
        try:
            requests.get("https://www.google.com", timeout=5)
            return True
        except requests.RequestException:
            return False

    def verify_video_url(self, url):
        try:
            response = requests.head(url, timeout=5)
            return response.status_code == 200
        except requests.RequestException:
            return False

    def stream_video(self, video_path):
        try:
            if not self.verify_video_url(video_path):
                logging.error(f"Video file not accessible: {video_path}")
                return False

            ffmpeg_command = [
                'ffmpeg',
                '-re',                     # Read input at native frame rate
                '-i', video_path,          # Input file
                '-c:v', 'libx264',         # Video codec
                '-preset', 'veryfast',     # Encoding preset
                '-b:v', '3000k',           # Video bitrate
                '-maxrate', '3000k',       # Maximum bitrate
                '-bufsize', '6000k',       # Buffer size
                '-pix_fmt', 'yuv420p',     # Pixel format
                '-g', '50',                # Keyframe interval
                '-c:a', 'aac',             # Audio codec
                '-b:a', '192k',            # Audio bitrate
                '-ar', '44100',            # Audio sample rate
                '-f', 'flv',               # Output format
                f'rtmp://a.rtmp.youtube.com/live2/{self.youtube_stream_key}'
            ]

            self.current_process = subprocess.Popen(
                ffmpeg_command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )

            self.current_process.wait()
            return self.current_process.returncode == 0

        except Exception as e:
            logging.error(f"Error streaming video: {str(e)}")
            return False

    def run_stream_loop(self):
        while self.is_running:
            if not self.check_internet_connection():
                logging.warning("No internet connection. Retrying in 30 seconds...")
                time.sleep(30)
                continue

            for video_path in self.video_paths:
                if not self.is_running:
                    break

                logging.info(f"Starting stream for: {video_path}")
                success = self.stream_video(video_path)

                if not success:
                    logging.warning(f"Stream failed for {video_path}. Retrying in 10 seconds...")
                    time.sleep(10)
                else:
                    logging.info(f"Stream completed for {video_path}")
                    time.sleep(5)  # Brief pause between videos

    def start(self):
        logging.info("Starting stream manager...")

        # Start the main streaming loop in a separate thread
        stream_thread = Thread(target=self.run_stream_loop)
        stream_thread.daemon = True
        stream_thread.start()

        # Keep the main thread alive with a lightweight loop
        try:
            while self.is_running:
                time.sleep(1)
        except KeyboardInterrupt:
            self.handle_shutdown(None, None)

if __name__ == "__main__":
    try:
        # Create a timestamp for the log file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        logging.info(f"Starting streaming session at {timestamp}")

        manager = StreamManager()
        manager.start()
    except Exception as e:
        logging.error(f"Fatal error: {str(e)}")
        sys.exit(1)
            self.handle_shutdown(None, None)

if __name__ == "__main__":
    manager = LoopingStreamManager()
    manager.start()
