import argparse
from pathlib import Path

import pandas as pd
import parfive

PATH = Path(__file__).parent

import bz2
import os
import shutil
import warnings

import numpy as np
import pandas as pd
from astropy.io import fits
from astropy.io.fits.verify import VerifyWarning
from astropy.wcs import WCS, FITSFixedWarning
from parfive import Downloader
from rclone_python import rclone

warnings.simplefilter("ignore", category=(VerifyWarning, FITSFixedWarning))
import time

from tqdm import tqdm

# Constants
LOCAL_DOWNLOAD_DIR = "frames"
LOCAL_PROCESSED_DIR = "processed_grids"
GDRIVE_REMOTE_NAME = "gdrive"  # This should match the name you configured in rclone
GDRIVE_DESTINATION_DIR = (
    "hermes/frames2"  # Replace with your desired destination folder on Google Drive
)
BATCH_SIZE = 100  # Number of files to process in each batch
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


def get_grid(filename, ra, dec):
    # Load the FITS file
    try:
        with bz2.open(filename, "rb") as f:
            hdulist = fits.open(f)
            header = hdulist[0].header
            data = hdulist[0].data

            # Check if data is 2D
            if data.ndim != 2:
                raise ValueError("Expected 2D FITS data.")

            # Get the pixel coordinates for the given RA and Dec
            wcs = WCS(header)
            x, y = wcs.world_to_pixel_values(ra, dec)

            # Extract scalar values if they are arrays
            x = x.item() if hasattr(x, "item") else x
            y = y.item() if hasattr(y, "item") else y

        # Convert to integers
        x, y = int(round(x)), int(round(y))

        # Define the grid boundaries
        xmin = max(0, x - 20)
        xmax = min(data.shape[1], x + 20)
        ymin = max(0, y - 20)
        ymax = min(data.shape[0], y + 20)

        # Extract the grid
        grid = data[ymin:ymax, xmin:xmax]

        # Determine the size of the grid to pad
        pad_y = max(0, 40 - (ymax - ymin))
        pad_x = max(0, 40 - (xmax - xmin))

        # Pad the grid to ensure it's 40x40
        padded_grid = np.pad(
            grid, ((0, pad_y), (0, pad_x)), mode="constant", constant_values=0
        )
        # Crop the padded grid to 40x40 if it exceeds the required size
        grid = padded_grid[:40, :40]

        return grid.astype(np.float32)

    except Exception:
        return None


def download_files(urls, band):
    """Download files using the provided downloader."""
    downloader = get_fits_downloader(urls, f"{LOCAL_DOWNLOAD_DIR}/{band}")
    files = downloader.download()

    # Check for any failed downloads
    if files.errors:
        with open(f"errors_{band}.txt", "a") as f:
            f.write("\n".join([f"{file.error} - {file.url}" for file in files.errors]))
    return files


# def process_file(file_info):
#     """Wrapper function to process a single file and return its grid and metadata."""
#     filename, ra, dec = file_info
#     grid = get_grid(filename, ra, dec)
#     return grid


def run_hail_mary(df, data_path):
    """Process the DataFrame in batches, download files, and upload them to Google Drive."""

    # Split the DataFrame into chunks
    for start in range(0, len(df), BATCH_SIZE):  # Start at 900 to avoid re-downloading
        # breakpoint()
        temp_df = df.iloc[start : start + BATCH_SIZE].copy().reset_index(drop=True)
        filenames = temp_df["filenames"]
        fits_url = temp_df["fits_url"]
        ra, dec = temp_df["ra"], temp_df["dec"]
        processed_name = f"{int(time.time())}.npz"  # Use a timestamp as the filename
        for x in BANDS:
            urls = fits_url.str.replace("frame-x-", f"frame-{x}-").tolist()
            # Download files
            files = download_files(urls, x)
            # Process the downloaded files
            print(f"Processing {len(files)} files")
            grids = []

            for index, file in tqdm(enumerate(files)):
                ra_index, dec_index = ra[index], dec[index]
                grid = get_grid(file, ra_index, dec_index)
                if grid is not None:
                    grids.append(grid)
                else:
                    with open(f"errors_{x}.txt", "a") as f:
                        f.write(f"Error processing {file}\n")

            # Convert grids to a numpy array
            grids_array = np.array(grids)

            # Convert DataFrame to dictionary
            metadata_dict = temp_df.to_dict()
            # breakpoint()

            # Save grids and metadata to a .npz file
            output_filename = f"{LOCAL_PROCESSED_DIR}/{x}/{processed_name}"  # Use a timestamp as the filename
            np.savez(output_filename, grids=grids_array, metadata=metadata_dict)

            # Upload the frames to Google Drive
            rclone.copy(
                f"./{LOCAL_DOWNLOAD_DIR}/{x}",
                f"gdrive:hermes/frames2/{x}/",
                ignore_existing=True,
                args=["-P", "--transfers=100", "--checkers=100"],
            )

            # Upload the frames to Google Drive
            rclone.copy(
                output_filename,
                f"gdrive:hermes/processed_grids/{x}/",
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
