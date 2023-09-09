# import requests
from zipfile import ZipFile
import os
import pandas as pd
from prefect import flow, task
import urllib.request


@task(persist_result=True)
def schedule_feed(schedule_url: str):
    """Get newest schedule GTFS file from Massachusets Bay Transportation Authority"""

    filename = "MBTA_GTFS.zip"

    urllib.request.urlretrieve(schedule_url, filename)

    with ZipFile(filename) as myzip:
        agency = pd.read_csv(myzip.open("agency.txt"), low_memory=False)
        routes = pd.read_csv(myzip.open("routes.txt"), low_memory=False)
        trip = pd.read_csv(myzip.open("trips.txt"), low_memory=False)
        calendar = pd.read_csv(myzip.open("calendar.txt"), low_memory=False)
        stop_times = pd.read_csv(myzip.open("stop_times.txt"), low_memory=False)
        # stops = pd.read_csv(myzip.open("stops.txt"), low_memory=False)

    # os.remove("MBTA_GTFS.zip")

    return agency, routes, trip, calendar, stop_times  # , stops


# @task
# def load_schedules_to_gcs(
#     prefect_gcs_block_name: str, from_path: str, to_path: str
# ) -> None:
#     """Load the trips today schedule to Google Cloud Bucket"""

#     gcs_block = GcsBucket.load(prefect_gcs_block_name)
#     gcs_block.upload_from_path(from_path=from_path, to_path=to_path)

#     os.remove(from_path)

#     return None


@flow
def schedules(
    schedule_url: str = "https://cdn.mbta.com/MBTA_GTFS.zip",
    # agency_name: str = "MBTA",
    # current_schedule_filename: str = "schedule_today",
    # prefect_gcs_block_name: str = "subway-gcs-bucket",
):
    schedule_feed(schedule_url)

    os.remove("MBTA_GTFS.zip")

    # load_schedules_to_gcs(
    #     wait_for=[trips_today],
    #     prefect_gcs_block_name=prefect_gcs_block_name,
    #     from_path=f"{current_schedule_filename}.parquet.gzip",
    #     to_path=f"current_schedule/{current_schedule_filename}.parquet.gzip",
    # )


if __name__ == "__main__":
    schedules()
