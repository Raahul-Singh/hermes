import glob
import os
from pathlib import Path

import pandas as pd

PATH = Path(__file__).parent
FITS_PATH = PATH / "fits/"


def update_df(df):
    files = glob.glob(str(FITS_PATH / "*.fits"))
    [os.path.basename(file) for file in files]
    breakpoint()


if __name__ == "__main__":
    df = pd.read_csv("test_run/data/test_query_1k_with_urls.csv")
    update_df(df)
