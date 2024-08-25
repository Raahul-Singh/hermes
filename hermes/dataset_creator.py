import os
import warnings

import numpy as np
import pandas as pd
from astropy.io import fits
from astropy.io.fits.verify import VerifyWarning
from astropy.wcs import WCS, FITSFixedWarning

warnings.simplefilter("ignore", (VerifyWarning, FITSFixedWarning))
import argparse
import logging

from sdss_access import Path
from tqdm import tqdm

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
# create file handler which logs even debug messages
fh = logging.FileHandler("info.log")
er = logging.FileHandler("error.log")
fh.setLevel(logging.INFO)
er.setLevel(logging.ERROR)
logger.addHandler(fh)
logger.addHandler(er)
import glob


def get_grid(file_path, filename, ra, dec, shape=(40, 40)):
    grids = []
    for j in ["u", "g", "r", "i", "z"]:
        current_band = filename.replace("frame-x-", f"frame-{j}-")
        with fits.open(f"{file_path}/{current_band}") as hdul:
            header = hdul[0].header
            data = hdul[0].data
            if "BSCALE" in header and "BZERO" in header:
                BSCALE = header["BSCALE"]
                BZERO = header["BZERO"]
                data = data * BSCALE + BZERO

        # Get the pixel coordinates for the given RA and Dec
        wcs = WCS(header)
        x, y = wcs.world_to_pixel_values(ra, dec)
        x = x.item() if hasattr(x, "item") else x
        y = y.item() if hasattr(y, "item") else y
        x, y = int(round(x)), int(round(y))

        # Define the grid boundaries
        height, width = shape
        xmin = max(0, x - width // 2)
        xmax = min(data.shape[1], x + width // 2)
        ymin = max(0, y - height // 2)
        ymax = min(data.shape[0], y + height // 2)

        # Extract the grid
        grid = data[ymin:ymax, xmin:xmax]

        # Determine the size of the grid to pad
        pad_y = max(0, height - (ymax - ymin))
        pad_x = max(0, width - (xmax - xmin))

        # Pad the grid to ensure it's 40x40
        padded_grid = np.pad(
            grid, ((0, pad_y), (0, pad_x)), mode="constant", constant_values=0
        )
        # Crop the padded grid to 40x40 if it exceeds the required size
        grid = padded_grid[:width, :height]

        grids.append(grid)
    return np.stack(grids, axis=-1).astype(np.float32)


def save_data(df, save_path, file_path):
    os.makedirs(f"{save_path}/X", exist_ok=True)
    os.makedirs(f"{save_path}/y", exist_ok=True)

    all_files = [f.split("/")[-1] for f in glob.glob(f"{file_path}/frame-*.fits")]
    for i, row in tqdm(df.iterrows(), total=df.shape[0]):
        files = []
        for j in ["u", "g", "r", "i", "z"]:
            files.append(row["file_name"].replace("frame-x-", f"frame-{j}-"))

        if not all([f in all_files for f in files]):
            logger.error(f"Files not found for {files}, skipping sample {i}")
            continue

        obj_id = row["objID"]
        y = row[["z", "zErr", "template_photo_z", "template_photo_zErr"]].astype(
            np.float32
        )

        try:
            grid = get_grid(file_path, row["file_name"], row["ra"], row["dec"])
            np.save(f"{save_path}/X/{obj_id}.npy", grid)
            np.save(f"{save_path}/y/{obj_id}.npy", y.values)
            logger.info(f"Sample {i} with {obj_id} Saved!")
        except Exception as e:
            logger.error(f"Error in Sample {i} with {obj_id}: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--save_path", type=str, default="processed_data")
    parser.add_argument(
        "--data_path", type=str, default="ordered_100k/extra_csv/speed_test_data.csv"
    )
    parser.add_argument(
        "--file_path", type=str, default="ordered_100k/eboss/photoObj/frames/301/94/6"
    )
    args = parser.parse_args()

    data = pd.read_csv(args.data_path, index_col=0)
    sdss_path = Path(release="dr17")
    data["file_name"] = data.apply(
        lambda row: sdss_path.url(
            "frame",
            run=int(row["run"]),
            rerun=int(row["rerun"]),
            camcol=int(row["camcol"]),
            field=int(row["field"]),
            filter="x",
        ).split("/")[-1],
        axis=1,
    )
    save_data(data.head(), args.save_path, args.file_path)
