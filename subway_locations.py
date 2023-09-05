from google.transit.gtfs_realtime_pb2 import FeedMessage
from datetime import datetime
import requests
import pandas as pd
from http import HTTPStatus
import pytz
import os
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket


@task(log_prints=True)
def et_live_locations_subway(filename: str) -> None:
    """Live bus data extracted from the Massachusets Bay Transportation Authority GTFS feed"""

    url = "https://cdn.mbta.com/realtime/VehiclePositions.pb"

    # requests will fetch the results from the url, which are the vehicle positions
    response = requests.get(url)

    # Get the data only if the HTTPStatus is OK
    if response.status_code == HTTPStatus.OK:
        message = FeedMessage()
        message.ParseFromString(response.content)

    # Pass and append the data to the list
    trips = []
    for t in message.entity:
        trips.append(t)

    # Unpack the nested data and expand to these columns
    rows = [
        {
            "id": t.id,
            "trip_id": t.vehicle.trip.trip_id,
            "start_time": t.vehicle.trip.start_time,
            "start_date": t.vehicle.trip.start_date,
            "schedule_relationship": t.vehicle.trip.schedule_relationship,
            "route_id": t.vehicle.trip.route_id,
            "direction_id": t.vehicle.trip.direction_id,
            "latitude": t.vehicle.position.latitude,
            "longitude": t.vehicle.position.longitude,
            "bearing": t.vehicle.position.bearing,
            "speed": t.vehicle.position.speed,
            "current_stop": t.vehicle.current_stop_sequence,
            "current_status": t.vehicle.current_status,
            "timestamp": datetime.utcfromtimestamp(t.vehicle.timestamp),
            "stop_id": t.vehicle.stop_id,
            "vehicle": t.vehicle.vehicle.id,
            "label": t.vehicle.vehicle.label,
        }
        for t in trips
    ]
    # Create a DataFrame with rows as data and dropping any duplicate data
    df_1 = pd.DataFrame(rows).drop_duplicates()

    # Timestamps are in UTC, tz_localize is used keep it UTC
    # and not switch to local time
    df_1["timestamp"] = df_1["timestamp"].dt.tz_localize("UTC")

    # Switch timestamp to local time of MBTA
    df_1["timestamp"] = df_1["timestamp"].dt.tz_convert(tz="US/Eastern")

    # Declare the format of the timestamp, including microseconds
    df_1["timestamp"] = pd.to_datetime(df_1["timestamp"], format="%Y-%m-%d %H:%M:%S.%f")

    # Define timezone data and get the date of the timezone
    tz = pytz.timezone("US/Eastern")
    today = datetime.now(tz).date()

    # Set the dataframe to data only for today
    df_2 = df_1[df_1["timestamp"].dt.date == today]

    # Rename fields to show how current it is
    df_3 = df_2.rename(
        {"start_date": "live_start_date", "route_id": "live_route_id"}, axis=1
    )

    # Convert start & end dates to datetime
    df_3["live_start_date"] = pd.to_datetime(df_3["live_start_date"], format="%Y-%m-%d")

    # These are route id's for subway lines
    subway_only = [
        "Blue",
        "Green-B",
        "Green-C",
        "Green-D",
        "Green-E",
        "Mattapan",
        "Orange",
        "Red",
    ]

    df_3 = df_3[df_3["live_route_id"].isin(subway_only)]

    # Convert the DataFrame to parquet file type that is compressed
    df_3.to_parquet(f"{filename}.parquet.gzip", engine="pyarrow", compression="gzip")

    return None


@task
def load_live_locations_subway_to_gcs(
    prefect_gcs_block_name: str, from_path: str, to_path: str
) -> None:
    """Load the mbta gtfs live locations to Google Cloud Bucket"""

    # Load the GCS block on Prefect
    gcs_block = GcsBucket.load(prefect_gcs_block_name)

    # Upload data to the GCS bucket
    gcs_block.upload_from_path(from_path=from_path, to_path=to_path)

    os.remove(from_path)

    return None


@flow
def flow_live_locations_subway(
    prefect_gcs_block_name: str = "subway-gcs-bucket",
    live_locations_filename: str = "live_location_subway",
):
    # Prefect task 1
    et_live_locations_subway(
        filename=live_locations_filename,
    )

    # Prefect task 2
    load_live_locations_subway_to_gcs(
        wait_for=[et_live_locations_subway],
        prefect_gcs_block_name=prefect_gcs_block_name,
        from_path=f"{live_locations_filename}.parquet.gzip",
        to_path=f"live_location/{live_locations_filename}.parquet.gzip",
    )


if __name__ == "__main__":
    flow_live_locations_subway()
