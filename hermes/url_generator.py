def get_frame_url(run, rerun, camcol, field, filter_val="r"):
    file_name = path.url(
        "frame", run=run, rerun=rerun, camcol=camcol, field=field, filter=filter_val
    ).split("/")[-1]
    base_url = (
        "https://data.sdss.org/sas/dr18/prior-surveys/sdss4-dr17-eboss/photoObj/frames"
    )
    return f"{base_url}/{rerun}/{run}/{camcol}/{file_name}.bz2"


def apply_to_df(df, filter_val="r"):
    df["fits_url"] = df.apply(
        lambda row: get_frame_url(
            row["run"], row["rerun"], row["camcol"], row["field"], filter_val
        ),
        axis=1,
    )
    return df
