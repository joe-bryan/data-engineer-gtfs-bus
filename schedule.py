from zipfile import ZipFile
import os
import pandas as pd
from prefect import flow, task
import urllib.request
import polars as pl
import numpy as np
import pytz
from datetime import datetime
from prefect_gcp.cloud_storage import GcsBucket


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
        stops = pd.read_csv(myzip.open("stops.txt"), low_memory=False)

    return agency, routes, trip, calendar, stops


@task(persist_result=True)
def stop_times_file(schedule_url: str):
    filename = "MBTA_GTFS.zip"

    urllib.request.urlretrieve(schedule_url, filename)

    with ZipFile(filename) as myzip:
        pl.read_csv(
            myzip.open("stop_times.txt"),
            dtypes={"trip_id": str, "stop_id": str, "stop_headsign": str},
        ).write_parquet(
            "stop_times.parquet.gzip", compression="gzip", row_group_size=100000
        )
        stop_times = pd.read_parquet(
            "stop_times.parquet.gzip",
            columns=[
                "trip_id",
                "arrival_time",
                "departure_time",
                "stop_id",
                "stop_sequence",
            ],
        )

    return stop_times


@task(persist_result=True)
def add_stops_stoptimes_schedule(
    agency: pd.DataFrame,
    routes: pd.DataFrame,
    trip: pd.DataFrame,
    calendar: pd.DataFrame,
    agency_name: str,
) -> pd.DataFrame:
    """Add stops and stop times to each trip for the selected agency"""

    # Set agency id and agency name to MBTA only
    agency_id = agency["agency_id"][agency["agency_name"] == agency_name].values[0]

    # Find associated routes for MBTA
    routes = routes[routes.agency_id == agency_id]

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

    routes = routes[routes["route_id"].isin(subway_only)]

    routes.drop(
        ["route_text_color", "route_sort_order", "listed_route"], axis=1, inplace=True
    )

    trip.drop(["trip_short_name"], axis=1, inplace=True)

    trips_routes = trip.merge(routes, how="left", on="route_id")

    # Replace empty values with NaN
    trips_routes["agency_id"].replace("", np.nan, inplace=True)

    # Assert and remove trips that aren't part of the selected agency_name
    trips_routes.dropna(subset=["agency_id"], inplace=True)

    # # Add calendar data to trips_routes
    trips_routes_dates = trips_routes.merge(calendar, how="left", on="service_id")

    return trips_routes_dates  # trips_routes_dates_stoptimes_stops


def stop_stop_times(trips_routes_dates: pd.DataFrame, stops: pd.DataFrame):
    stop_times_pl = pl.read_parquet(
        "stop_times.parquet.gzip",
        columns=[
            "trip_id",
            "arrival_time",
            "departure_time",
            "stop_id",
            "stop_sequence",
        ],
    )

    stops.drop(["on_street", "at_street", "stop_address"], axis=1, inplace=True)

    trips_routes_dates_pl = pl.from_pandas(trips_routes_dates)

    stops_pl = pl.from_pandas(stops)

    # Add stop times data to trips_routes_dates
    trips_routes_dates_stoptimes = trips_routes_dates_pl.join(
        stop_times_pl, left_on="trip_id", right_on="trip_id"
    )

    # # Add stops data to trips_routes_dates_stoptimes
    # trips_routes_dates_stoptimes_stops = trips_routes_dates_stoptimes.join(
    #     stops_pl, left_on="stop_id", right_on="stop_id"
    # )

    trips_routes_dates_stoptimes = trips_routes_dates_stoptimes.to_pandas()

    return trips_routes_dates_stoptimes


# @task
# def schedule_today(
#     trips_routes_dates_stoptimes_stops: pd.DataFrame, current_trips_filename: str
# ) -> pd.DataFrame:
#     """Transform all trip schedules to include only those running on the current (US/Eastern) day"""

#     # Set the timezone as UTC
#     tz = pytz.timezone("US/Eastern")

#     # Get the datetime of today
#     todays_date = datetime.now(tz)

#     # # Get only the day of week of today in lowercase
#     # current_day = todays_date.strftime("%A").lower()

#     # Get the date in 'YearMonthDay' format
#     todays_date_string = todays_date.strftime("%Y%m%d")

#     # Convert todays_date_string to a Pandas datetime
#     todays_date_1 = pd.to_datetime(todays_date_string, format="%Y%m%d")

#     # Convert start & end dates to datetime
#     trips_routes_dates_stoptimes_stops[
#         ["start_date", "end_date"]
#     ] = trips_routes_dates_stoptimes_stops[["start_date", "end_date"]].apply(
#         pd.to_datetime, format="%Y%m%d"
#     )

#     # Use these columns only
#     columns_only = [
#         "route_id",
#         "service_id",
#         "trip_id",
#         "trip_headsign",
#         "direction_id",
#         "block_id",
#         "shape_id",
#         "wheelchair_accessible",
#         "route_pattern_id",
#         "bikes_allowed",
#         "agency_id",
#         "route_short_name",
#         "route_long_name",
#         "route_desc",
#         "route_type",
#         "route_url",
#         "route_color",
#         "route_text_color",
#         "route_sort_order",
#         "route_fare_class",
#         "line_id",
#         "network_id",
#         "monday",
#         "tuesday",
#         "wednesday",
#         "thursday",
#         "friday",
#         "saturday",
#         "sunday",
#         "start_date",
#         "end_date",
#         "arrival_time",
#         "departure_time",
#         "stop_id",
#         "stop_sequence",
#         "stop_code",
#         "stop_name",
#         "stop_desc",
#         "platform_code",
#         "platform_name",
#         "stop_lat",
#         "stop_lon",
#         "zone_id",
#         "stop_url",
#         "level_id",
#         "location_type",
#         "parent_station",
#         "wheelchair_boarding",
#         "municipality",
#         "vehicle_type",
#     ]

#     # Only use these columns in the dataset
#     trips_routes_dates_stoptimes_stops_1 = trips_routes_dates_stoptimes_stops[
#         columns_only
#     ]

#     # Only use data if current day is in the service interval
#     trips_today = trips_routes_dates_stoptimes_stops_1[
#         (todays_date_1 >= trips_routes_dates_stoptimes_stops_1["start_date"])
#         & (todays_date_1 <= trips_routes_dates_stoptimes_stops_1["end_date"])
#     ]

#     # Apply string type to stop_id in order to successfully
#     # merge with df_3
#     trips_today["stop_id"] = trips_today["stop_id"].apply(str)

#     # Save and compress to parquet file type
#     trips_today.to_parquet(f"{current_trips_filename}.parquet.gzip", compression="gzip")

#     return trips_today


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
    agency_name: str = "MBTA",
    # current_schedule_filename: str = "schedule_today",
    # prefect_gcs_block_name: str = "subway-gcs-bucket",
):
    agency, routes, trip, calendar, stops = schedule_feed(schedule_url)

    stop_times_file(schedule_url)

    # trips_stops =
    trips_routes_dates = add_stops_stoptimes_schedule(
        # wait_for=[agency, routes, trip, calendar, stops, stop_times],
        agency=agency,
        routes=routes,
        trip=trip,
        calendar=calendar,
        agency_name=agency_name,
    )

    stop_stop_times(trips_routes_dates=trips_routes_dates, stops=stops)

    # # trips_today = schedule_today(
    # schedule_today(
    #     wait_for=[trips_stops],
    #     trips_routes_dates_stoptimes_stops=trips_stops,
    #     current_trips_filename=current_schedule_filename,
    # )

    # load_schedules_to_gcs(
    #     wait_for=[trips_today],
    #     prefect_gcs_block_name=prefect_gcs_block_name,
    #     from_path=f"{current_schedule_filename}.parquet.gzip",
    #     to_path=f"current_schedule/{current_schedule_filename}.parquet.gzip",
    # )

    os.remove("MBTA_GTFS.zip")

    os.remove("stop_times.parquet.gzip")


if __name__ == "__main__":
    schedules()
