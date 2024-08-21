import argparse
from pathlib import Path

import pandas as pd
import parfive

PATH = Path(__file__).parent

import os
import shutil

import pandas as pd
from parfive import Downloader
from rclone_python import rclone

# Constants
LOCAL_DOWNLOAD_DIR = "frames"
GDRIVE_REMOTE_NAME = "gdrive"  # This should match the name you configured in rclone
GDRIVE_DESTINATION_DIR = (
    "hermes/frames2"  # Replace with your desired destination folder on Google Drive
)
BATCH_SIZE = 500  # Number of files to process in each batch
BANDS = ["u", "g", "r", "i", "z"]

# Ensure the local download directory exists

for x in BANDS:
    os.makedirs(f"{LOCAL_DOWNLOAD_DIR}/{x}", exist_ok=True)


def get_fits_downloader(urls, outdir, max_conn=500):
    """Create a parfive downloader for FITS files."""
    dl = Downloader(max_conn=max_conn)
    for url in urls:
        dl.enqueue_file(url, path=outdir)
    return dl


def download_files(urls, band):
    """Download files using the provided downloader."""
    downloader = get_fits_downloader(urls, f"{LOCAL_DOWNLOAD_DIR}/{band}")
    files = downloader.download()

    # Check for any failed downloads
    if files.errors:
        with open(f"errors_{band}.txt", "a") as f:
            f.write("\n".join([f"{file.error} - {file.url}" for file in files.errors]))
    return files


def run_hail_mary(df, data_path):
    """Process the DataFrame in batches, download files, and upload them to Google Drive."""

    # Split the DataFrame into chunks
    for start in range(0, len(df), BATCH_SIZE):  # Start at 900 to avoid re-downloading
        # breakpoint()
        temp_df = df.iloc[start : start + BATCH_SIZE]
        filenames = temp_df["filenames"]
        fits_url = temp_df["fits_url"]
        for x in BANDS:
            urls = fits_url.str.replace("frame-x-", f"frame-{x}-").tolist()
            # Download files
            download_files(urls, x)

            rclone.copy(
                f"./{LOCAL_DOWNLOAD_DIR}/{x}",
                f"gdrive:hermes/frames2/{x}/",
                ignore_existing=True,
                args=["-P", "--transfers=100", "--checkers=100"],
            )

            shutil.rmtree(f"{LOCAL_DOWNLOAD_DIR}/{x}", ignore_errors=True)
            os.makedirs(f"{LOCAL_DOWNLOAD_DIR}/{x}", exist_ok=True)

        df.loc[df["filenames"].isin(filenames), "file_downloaded"] = 1
        df.to_csv(data_path, index=False)
        print(f"Batch {start} complete")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download Upload fits files")
    parser.add_argument("--data_path", type=str, default="first75k_dataset.csv")
    parser.add_argument("--max_conn", type=int, default=100)
    args = parser.parse_args()

    df = pd.read_csv(args.data_path)
    df = df[
        df["file_downloaded"] == 0
    ]  # Only process files that haven't been downloaded yet
    run_hail_mary(df, data_path=args.data_path)
