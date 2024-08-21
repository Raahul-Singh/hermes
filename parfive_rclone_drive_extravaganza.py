import argparse
from pathlib import Path

import pandas as pd
import parfive

PATH = Path(__file__).parent

import os
import shutil
import subprocess

import pandas as pd
from parfive import Downloader
from rclone_python import rclone

# Constants
LOCAL_DOWNLOAD_DIR = "fits_temp_storage"
GDRIVE_REMOTE_NAME = "gdrive"  # This should match the name you configured in rclone
GDRIVE_DESTINATION_DIR = (
    "hermes/75k"  # Replace with your desired destination folder on Google Drive
)
BATCH_SIZE = 100  # Number of files to process in each batch

# Ensure the local download directory exists
os.makedirs(LOCAL_DOWNLOAD_DIR, exist_ok=True)


def get_fits_downloader(urls, outdir, max_conn=100):
    """Create a parfive downloader for FITS files."""
    dl = Downloader(max_conn=max_conn)
    for url in urls:
        dl.enqueue_file(url, path=outdir)
    return dl


def download_files(urls):
    """Download files using the provided downloader."""
    downloader = get_fits_downloader(urls, LOCAL_DOWNLOAD_DIR)
    files = downloader.download()

    # Check for any failed downloads
    if files.errors:
        for error in files.errors:
            print(f"Error downloading {error.url}: {error.exception}")

    return files


def run_hail_mary(u, g, r, i, z):
    """Process the DataFrame in batches, download files, and upload them to Google Drive."""

    # Split the DataFrame into chunks
    for start in range(
        2310, len(u), BATCH_SIZE
    ):  # Start at 900 to avoid re-downloading
        u_urls = u[start : start + BATCH_SIZE]
        g_urls = g[start : start + BATCH_SIZE]
        r_urls = r[start : start + BATCH_SIZE]
        i_urls = i[start : start + BATCH_SIZE]
        z_urls = z[start : start + BATCH_SIZE]
        file_urls = u_urls + g_urls + r_urls + i_urls + z_urls

        # Download files
        download_files(file_urls)

        # Upload each file to Google Drive
        subprocess.run(
            ["rclone", "mkdir", f"gdrive:hermes/batch_post_900/batch_{start}/"]
        )
        rclone.copy(
            f"./{LOCAL_DOWNLOAD_DIR}/",
            f"gdrive:hermes/batch_post_900/batch_{start}/",
            ignore_existing=True,
            args=["-P", "--transfers=100"],
        )

        shutil.rmtree(f"{LOCAL_DOWNLOAD_DIR}/", ignore_errors=True)
        print(f"Batch {start} complete")
        os.makedirs(LOCAL_DOWNLOAD_DIR, exist_ok=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download Upload fits files")
    parser.add_argument(
        "--data_path", type=str, default="data_lt_1_sampled_75_k_rows_updated.csv"
    )
    parser.add_argument("--max_conn", type=int, default=100)
    args = parser.parse_args()

    df = pd.read_csv(args.data_path)
    df = df.drop_duplicates(
        subset=["fits_url_z"]
    )  # Remove duplicates for u, g, r, i, z All same
    urls = []

    run_hail_mary(
        df["fits_url_u"].tolist(),
        df["fits_url_g"].tolist(),
        df["fits_url_r"].tolist(),
        df["fits_url_i"].tolist(),
        df["fits_url_z"].tolist(),
    )
