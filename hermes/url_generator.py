import argparse

import pandas as pd
from sdss_access import Path

sdss_path = Path(release="dr17")


def get_fits_url(run, rerun, camcol, field, filter_val="r"):
    file_name = sdss_path.url(
        "frame", run=run, rerun=rerun, camcol=camcol, field=field, filter=filter_val
    ).split("/")[-1]
    base_url = (
        "https://data.sdss.org/sas/dr18/prior-surveys/sdss4-dr17-eboss/photoObj/frames"
    )
    return f"{base_url}/{rerun}/{run}/{camcol}/{file_name}.bz2"


def apply_fits_url_to_df(df, filter_val="r"):
    df[f"fits_url_{filter_val}"] = df.apply(
        lambda row: get_fits_url(
            row["run"], row["rerun"], row["camcol"], row["field"], filter_val
        ),
        axis=1,
    )
    return df


def get_jpeg_url(ra, dec, scale=0.396, width=64, height=64):
    base_url = "http://skyserver.sdss.org/dr17/SkyServerWS/ImgCutout/getjpeg?"
    return f"{base_url}ra={ra}&dec={dec}&scale={scale}&width={width}&height={height}"


def apply_jpeg_url_to_df(df, scale=0.396, height=40, width=40):
    df["jpeg_url"] = df.apply(
        lambda row: get_jpeg_url(
            row["ra"], row["dec"], scale=scale, width=width, height=height
        ),
        axis=1,
    )
    return df


def add_filename(df):
    df["filename"] = df["fits_url_u"].apply(
        lambda x: x.split("/")[-1][:-4]
    )  # remove .bz2
    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate URL to download fits and jpg files"
    )
    parser.add_argument("--data_path", type=str, default="represenative_data.csv")
    args = parser.parse_args()

    df = pd.read_csv(args.data_path, index_col=0)
    for i in ["u"]:  # , "g", "r", "i", "z"]:
        df = apply_fits_url_to_df(df, filter_val=i)
    # df = apply_jpeg_url_to_df(df)
    df = add_filename(df)
    df.to_csv(f"{args.data_path[:-4]}_updated.csv")
    print(df.head())
