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
    df = pd.read_csv("test_run/data/test_query_1k_with_urls.csv")
    fits_urls = df["fits_url"].tolist()
    outdir = PATH / "fits/"
    dl = get_fits_downloader(fits_urls, outdir)
    dl.download()
