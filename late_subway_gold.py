from google.cloud import storage
import pandas as pd
from io import StringIO
from prefect import task, flow
from prefect_gcp.cloud_storage import GcsBucket
from pathlib import Path
import os

client = storage.Client()


@task
def read_csvfile(bucket_name: str, file_path: str):
    """Retrieve GCS bucket content"""

    bucket = client.bucket(bucket_name)
    content = bucket.blob(file_path).download_as_string().decode("utf-8")

    return content


@task
def access_dataframe_from_gcsbucket(dataframe: pd.DataFrame):
    """Reads the GCS bucket csv file and converts to a Pandas DataFrame"""

    dataframe = pd.read_csv(StringIO(dataframe))

    return dataframe


@task
def transform_dataframe(
    dataframe: pd.DataFrame,
    column1: str,
    column2: str,
    column3: str,
    column4: str,
    column5: str,
):
    """Make changes to the dataframe to improve the visuals of the map"""
    dataframe[column1] = dataframe[column1].apply(
        lambda x: pd.to_datetime(x, format=("%H:%M:%S"))
    )

    dataframe[column1] = dataframe[column1].apply(lambda x: x.strftime("%H:%M %p"))

    dataframe[column2] = dataframe[column2].apply(
        lambda x: pd.to_datetime(x, format="%Y-%m-%d %H:%M:%S.%f")
    )

    dataframe[column3] = dataframe[column3].apply(
        lambda x: pd.to_datetime(x, format="%Y-%m-%d %H:%M:%S.%f")
    )

    dataframe[column4] = dataframe[column3] - dataframe[column2]

    dataframe[column4] = (
        dataframe[column4]
        .dt.total_seconds()
        .apply(lambda s: f"{(s % 3600) // 60:02.0f}:{s % 60:02.0f}")
    )

    dataframe[column5] = dataframe[column4].astype(str)

    dataframe[column5] = (
        dataframe[column5].str[:2]
        + " minutes and "
        + dataframe[column5].str[3:]
        + " seconds"
    )

    return dataframe


@task()
def load_late_subways_gold_to_gcs(
    late_subways_path: Path, prefect_gcs_block_name: str
) -> None:
    """Upload late subways to GCS"""

    gcs_block = GcsBucket.load(prefect_gcs_block_name)
    gcs_block.upload_from_path(from_path=late_subways_path)

    return None


@flow
def gold_flow(
    bucket_name: str = "subway-mbta-location",
    file_path: str = "late_subways.csv",
    prefect_gcs_block_name: str = "subway-gcs-bucket",
):
    data = read_csvfile(bucket_name=bucket_name, file_path=file_path)

    csv_data = access_dataframe_from_gcsbucket(dataframe=data)

    gold_csv = transform_dataframe(
        dataframe=csv_data,
        column1="arrival_time",
        column2="arrival_time_fixed",
        column3="timestamp",
        column4="late _",
        column5="late by",
    )

    gold_csv.to_csv("late_subways_gold.csv", index=False)

    load_late_subways_gold_to_gcs(
        wait_for=[gold_csv],
        late_subways_path="late_subways_gold.csv",
        prefect_gcs_block_name=prefect_gcs_block_name,
    )

    os.remove("late_subways_gold.csv")


if __name__ == "__main__":
    gold_flow()
