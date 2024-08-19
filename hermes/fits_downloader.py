import argparse
from pathlib import Path

import pandas as pd
import parfive

PATH = Path(__file__).parent


def get_fits_downloader(urls, outdir, max_conn=8):
    dl = parfive.Downloader(max_conn=max_conn)
    for url in urls:
        dl.enqueue_file(url, path=outdir)
    return dl


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download fits files")
    parser.add_argument("--download_single_bands", action="store_true")
    parser.add_argument("--outdir", type=str, default=f"{str(PATH)}/fits/")
    parser.add_argument(
        "--data_path", type=str, default="test_run/data/initial_download_updated.csv"
    )
    parser.add_argument("--max_conn", type=int, default=8)
    args = parser.parse_args()

    df = pd.read_csv(args.data_path).iloc[:2]
    urls = []

    if args.download_single_bands:
        urls = df["fits_url_r"].tolist()
    else:
        urls.extend(df["fits_url_u"].tolist())
        urls.extend(df["fits_url_g"].tolist())
        urls.extend(df["fits_url_r"].tolist())
        urls.extend(df["fits_url_i"].tolist())
        urls.extend(df["fits_url_z"].tolist())

    outdir = args.outdir

    dl = get_fits_downloader(urls, outdir, max_conn=args.max_conn)
    dl.download()
