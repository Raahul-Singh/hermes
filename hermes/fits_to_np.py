import os
import warnings
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import numpy as np
import pandas as pd
from astropy.io import fits
from astropy.io.fits.verify import VerifyWarning
from astropy.wcs import WCS
from tqdm import tqdm

warnings.simplefilter("ignore", category=VerifyWarning)

PATH = Path(__file__).parent
FITS_PATH = PATH / "fits/"


class FITSProcessor:
    def __init__(self, file_list, width=40, height=40, ra_list=None, dec_list=None):
        self.file_list = file_list
        self.width = width
        self.height = height
        self.ra_list = ra_list
        self.dec_list = dec_list

    def get_coords(self, filename, ra, dec):
        # Open the FITS file
        try:
            with fits.open(filename) as hdul:
                # Access the primary header and data
                header = hdul[0].header
                hdul[0].data
        except FileNotFoundError as e:
            print(f"File Not found: {filename}: {e}")
            return -1, -1
        # Initialize WCS with the FITS header
        wcs = WCS(header)

        # Convert RA and Dec to pixel coordinates
        floating_pixel_coords = wcs.world_to_pixel_values(ra, dec)

        pixel_indices = (
            int(np.round(floating_pixel_coords[0])),
            int(np.round(floating_pixel_coords[1])),
        )

        # Unpack pixel indices
        x_center, y_center = pixel_indices
        return x_center, y_center

    def _process_fits_file(self, args):
        filename, ra, dec = args

        # Open the FITS file
        with fits.open(filename) as hdul:
            # Access the primary header and data
            header = hdul[0].header
            data = hdul[0].data

        # Initialize WCS with the FITS header
        wcs = WCS(header)

        # Convert RA and Dec to pixel coordinates
        floating_pixel_coords = wcs.world_to_pixel_values(ra, dec)

        # # Check for NaN values in the pixel coordinates
        # if np.any(np.isnan(floating_pixel_coords)):
        #     breakpoint()

        pixel_indices = (
            int(np.round(floating_pixel_coords[0])),
            int(np.round(floating_pixel_coords[1])),
        )

        # Unpack pixel indices
        x_center, y_center = pixel_indices

        # Calculate half dimensions
        half_width = self.width // 2
        half_height = self.height // 2

        # Calculate boundaries with clipping to ensure they stay within array bounds
        xmin = max(0, x_center - half_width)
        xmax = xmin + self.width
        if xmax > data.shape[1]:
            xmax = data.shape[1]
            xmin = xmax - self.width

        ymin = max(0, y_center - half_height)
        ymax = ymin + self.height
        if ymax > data.shape[0]:
            ymax = data.shape[0]
            ymin = ymax - self.height

        # Create the cutout
        cutout = data[ymin:ymax, xmin:xmax]

        return filename, cutout

    def process_files(self, output_dir="cutout_100k", output_file="cutouts.npz"):
        # Ensure the output directory exists
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        cutouts = []
        file_map = {}
        skipped_files = []

        # Prepare arguments for parallel processing
        args_list = [
            (filename, ra, dec)
            for filename, ra, dec in zip(self.file_list, self.ra_list, self.dec_list)
        ]

        # Use ProcessPoolExecutor for parallel processing
        with ProcessPoolExecutor() as executor:
            # Wrap the executor.map call with tqdm for a progress bar
            results = list(
                tqdm(
                    executor.map(self._process_fits_file, args_list),
                    total=len(self.file_list),
                    desc="Processing FITS files",
                )
            )

        # Collect results and build the mapping
        for idx, (filename, cutout) in enumerate(results):
            if cutout is None:
                breakpoint()
                skipped_files.append(filename)
            else:
                cutouts.append(cutout)
                file_map[os.path.basename(filename)] = idx

        # Save the cutouts and file map to a .npz file in the specified directory
        np.savez(
            os.path.join(output_dir, output_file),
            cutouts=np.array(cutouts),
            file_map=file_map,
        )

        # Log skipped files
        if skipped_files:
            with open(os.path.join(output_dir, "skipped_files.log"), "w") as log_file:
                log_file.write("\n".join(skipped_files))
            print(
                f"Skipped {len(skipped_files)} files due to NaN coordinates. See 'skipped_files.log' for details."
            )

    def process_files_seq_old(self, output_dir="cutouts", output_file="cutouts.npz"):
        # Ensure the output directory exists
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        cutouts = []
        file_map = {}
        skipped_files = []

        # Iterate over each file and process them sequentially
        for idx, (filename, ra, dec) in enumerate(
            zip(self.file_list, self.ra_list, self.dec_list)
        ):
            cutout = self._process_fits_file((filename, ra, dec))
            if cutout is None:
                skipped_files.append(filename)
            else:
                cutouts.append(cutout)
                file_map[os.path.basename(filename)] = idx

        # # Save the cutouts and file map to a .npz file in the specified directory
        # np.savez(os.path.join(output_dir, output_file), cutouts=np.array(cutouts), file_map=file_map)

        # Log skipped files
        # breakpoint()
        if skipped_files:
            breakpoint()
            with open(os.path.join(output_dir, "skipped_files.log"), "w") as log_file:
                log_file.write("\n".join(skipped_files))
            print(
                f"Skipped {len(skipped_files)} files due to NaN coordinates. See 'skipped_files.log' for details."
            )

    def process_files_seq(self, file_list, ra_list, dec_list):
        coords_df = pd.DataFrame(
            columns=["filename", "ra", "dec", "x_center", "y_center"]
        )
        for idx, (filename, ra, dec) in enumerate(zip(file_list, ra_list, dec_list)):
            x_center, y_center = self.get_coords(filename, ra, dec)
            coords_df.loc[idx] = [filename, ra, dec, x_center, y_center]
        coords_df.to_csv("coords.csv", index=False)


if __name__ == "__main__":
    # Load the DataFrame
    df = pd.read_csv(PATH / "test_run/data/test_query_1k_old.csv")

    ra_list = df["ra"].tolist()
    dec_list = df["dec"].tolist()
    file_list = df.apply(lambda row: f"{FITS_PATH}/{row['filename']}", axis=1).tolist()

    # Create the processor instance
    processor = FITSProcessor(
        file_list, width=40, height=40, ra_list=ra_list, dec_list=dec_list
    )

    # Process the files
    processor.process_files_seq(file_list, ra_list, dec_list)
