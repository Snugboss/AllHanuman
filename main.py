import os
import sys
import time
import logging
import ffmpeg
import imageio_ffmpeg

# --- Configuration ---
# Configure logging for clear output
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('streaming.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

# List of video URLs to stream
VIDEO_URLS = [
    "https://hanuman.s3.us-south.cloud-object-storage.appdomain.cloud/0000.mp4",
    "https://hanuman.s3.us-south.cloud-object-storage.appdomain.cloud/0001.mp4",
    "https://hanuman.s3.us-south.cloud-object-storage.appdomain.cloud/0002.mp4",
]

# Get YouTube stream key from environment variable or use a default
YOUTUBE_STREAM_KEY = os.getenv("YOUTUBE_STREAM_KEY", "jag4-c49f-8qx6-5mu5-9wg8")

def get_ffmpeg_executable():
    """
    Gets the path to the ffmpeg executable downloaded by imageio-ffmpeg.
    This is the key to making this a self-contained Python solution.
    """
    try:
        ffmpeg_path = imageio_ffmpeg.get_ffmpeg_exe()
        logging.info(f"Found FFmpeg executable at: {ffmpeg_path}")
        return ffmpeg_path
    except Exception as e:
        logging.critical(f"Could not get FFmpeg executable path from imageio-ffmpeg: {e}")
        logging.critical("Please ensure 'imageio-ffmpeg' is installed correctly (`pip install imageio-ffmpeg`).")
        return None

def stream_videos(video_urls, stream_key, ffmpeg_path):
    """
    Streams a list of videos to YouTube in a loop using ffmpeg-python,
    explicitly specifying the path to the ffmpeg executable.
    """
    if not stream_key or stream_key == "cr8p-6zjj-xeux-4cxa-fe5x":
        logging.warning("Using a hardcoded or default stream key.")
    
    rtmp_url = f'rtmp://a.rtmp.youtube.com/live2/{stream_key}'
    
    while True:
        for video_url in video_urls:
            logging.info(f"Starting stream for video: {video_url}")
            try:
                # Build the ffmpeg command using ffmpeg-python
                process = (
                    ffmpeg
                    .input(video_url, re=None)
                    .output(
                        rtmp_url,
                        format='flv',
                        vcodec='libx264',
                        preset='veryfast',
                        video_bitrate='3000k',
                        acodec='aac',
                        audio_bitrate='192k',
                        # Crucial parameters for stable streaming
                        reconnect=1,
                        reconnect_at_eof=1,
                        reconnect_streamed=1,
                        reconnect_delay_max=5
                    )
                    # Use run_async to manage the process and get logs
                    .run_async(cmd=ffmpeg_path, pipe_stdout=True, pipe_stderr=True)
                )
                
                # Log ffmpeg's output in real-time
                stdout, stderr = process.communicate()
                
                if process.returncode == 0:
                    logging.info(f"Finished streaming successfully: {video_url}")
                else:
                    logging.error(f"Streaming failed for {video_url} with return code: {process.returncode}")
                    # Decode stderr to see the actual error from ffmpeg
                    error_message = stderr.decode('utf-8', errors='ignore')
                    logging.error(f"FFmpeg error output:\n{error_message}")

            except ffmpeg.Error as e:
                logging.error(f"An ffmpeg.Error occurred for {video_url}: {e}")
                error_message = e.stderr.decode('utf-8', errors='ignore') if e.stderr else "No stderr."
                logging.error(f"FFmpeg stderr:\n{error_message}")
            except Exception as e:
                logging.error(f"An unexpected error occurred while streaming {video_url}: {e}")

            logging.info("Waiting 7 seconds before the next video...")
            time.sleep(7)

        logging.info("Completed a full loop of all videos. Restarting...")

if __name__ == "__main__":
    try:
        logging.info("--- Starting YouTube Streaming Script ---")
        ffmpeg_executable = get_ffmpeg_executable()
        if ffmpeg_executable:
            stream_videos(VIDEO_URLS, YOUTUBE_STREAM_KEY, ffmpeg_executable)
        else:
            logging.critical("Could not find FFmpeg. Exiting.")
            sys.exit(1)
    except KeyboardInterrupt:
        logging.info("\n--- Streaming stopped by user ---")
        sys.exit(0)
    except Exception as e:
        logging.critical(f"A fatal, unhandled error occurred: {e}")
        sys.exit(1)
