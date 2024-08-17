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
    df["fits_url"] = df.apply(
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


if __name__ == "__main__":
    df = pd.read_csv("test_run/data/test_query_1k.csv", index_col=0)
    df = apply_fits_url_to_df(df)
    df = apply_jpeg_url_to_df(df)
    df.to_csv("test_run/data/test_query_1k_with_urls.csv", index=False)
    print(df.head())
