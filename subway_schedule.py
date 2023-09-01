import requests
import os
import gtfs_kit as gk
import numpy as np
import pytz
from datetime import datetime
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket


@task()
def schedule_gtfs_feed() -> gk.feed:
    """Get newest schedule GTFS file from Massachusets Bay Transportation Authority"""

    schedule_url: str = "https://cdn.mbta.com/MBTA_GTFS.zip"

    r = requests.get(schedule_url)
    with open("gtfs_timetables.zip", "wb") as fd:
        for chunk in r.iter_content(chunk_size=128):
            fd.write(chunk)

    feed = gk.read_feed("gtfs_timetables.zip", dist_units="mi")

    os.remove("gtfs_timetables.zip")

    return feed


# @task()
# def add_stops_stoptimes_schedule(feed: gk.feed, agency_name: str) -> pd.DataFrame:
#     """Add stops and stop times to each trip for the selected agency"""

#     # Set agency id and agency name to MBTA only
#     agency_id = feed.agency["agency_id"][
#         feed.agency["agency_name"] == agency_name
#     ].values[0]

#     # Find associated routes for MBTA
#     routes = feed.routes[feed.routes.agency_id == agency_id]

#     # Define the trips
#     # Add routes data with trips
#     trips = feed.trips
#     trips_routes = trips.merge(routes, how="left", on="route_id")

#     # Replace empty values with NaN
#     trips_routes["agency_id"].replace("", np.nan, inplace=True)

#     # Assert and remove trips that aren't part of the selected agency_name
#     trips_routes.dropna(subset=["agency_id"], inplace=True)

#     # Add calendar_dates data to trips_routes
#     calendar_dates = feed.calendar
#     trips_routes_dates = trips_routes.merge(calendar_dates, how="left", on="service_id")

#     # Add stop times data to trips_routes_dates
#     stop_times = feed.stop_times
#     trips_routes_dates_stoptimes = trips_routes_dates.merge(
#         stop_times, how="left", on="trip_id"
#     )

#     # Add stops data to trips_routes_dates_stoptimes
#     stops = feed.stops
#     trips_routes_dates_stoptimes_stops = trips_routes_dates_stoptimes.merge(
#         stops, how="left", on="stop_id"
#     )

#     return trips_routes_dates_stoptimes_stops


# @task()
# def schedule_today(
#     trips_routes_dates_stoptimes_stops: pd.DataFrame, current_trips_filename: str
# ) -> pd.DataFrame:
#     """Transform all trip schedules to include only those running on the current (US/Eastern) day"""

#     # Set the timezone as UTC
#     tz = pytz.timezone("US/Eastern")

#     # Get the datetime of today
#     todays_date = datetime.now(tz)

#     # Get only the day of week of today in lowercase
#     current_day = todays_date.strftime("%A").lower()

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

#     # These are route id's for subway lines
#     subway_only = [
#         "Blue",
#         "Green-B",
#         "Green-C",
#         "Green-D",
#         "Green-E",
#         "Mattapan",
#         "Orange",
#         "Red",
#     ]

#     # Only use these subway routes in the dataset
#     trips_routes_dates_stoptimes_stops = trips_routes_dates_stoptimes_stops[
#         trips_routes_dates_stoptimes_stops["route_id"].isin(subway_only)
#     ]

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
#         "pickup_type",
#         "drop_off_type",
#         "timepoint",
#         "checkpoint_id",
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
#     trips_routes_dates_stoptimes_stops = trips_routes_dates_stoptimes_stops[
#         columns_only
#     ]

#     # Only use data if current day is in the service interval
#     trips_routes_dates_stoptimes_stops_1 = trips_routes_dates_stoptimes_stops[
#         (todays_date_1 >= trips_routes_dates_stoptimes_stops["start_date"])
#         & (todays_date_1 <= trips_routes_dates_stoptimes_stops["end_date"])
#     ]

#     # Check if trip is valid on the same day of week
#     trips_today = trips_routes_dates_stoptimes_stops_1[
#         trips_routes_dates_stoptimes_stops_1[current_day] == 1
#     ]

#     # Save and compress to parquet file type
#     trips_today.to_parquet(f"{current_trips_filename}.parquet.gzip", compression="gzip")

#     return trips_today


# @task()
# def load_schedule_to_gcs(
#     prefect_gcs_block_name: str, from_path: str, to_path: str
# ) -> None:
#     """Load the trips today schedule to Google Cloud Bucket"""

#     gcs_block = GcsBucket.load(prefect_gcs_block_name)
#     gcs_block.upload_from_path(from_path=from_path, to_path=to_path)

#     os.remove(from_path)

#     return None


@flow()
def get_gtfs_subway_schedule(
    # schedule_url: str = "https://cdn.mbta.com/MBTA_GTFS.zip",
    # agency_name: str = "MBTA",
    # current_schedule_filename: str = "schedule_today",
    # prefect_gcs_block_name: str = "subway-gcs-bucket",
) -> None:
    full_schedule = schedule_gtfs_feed()

    # trips_stops = add_stops_stoptimes_schedule(
    #     wait_for=[full_schedule], feed=full_schedule, agency_name=agency_name
    # )

    # trips_today = schedule_today(
    #     wait_for=[trips_stops],
    #     trips_routes_dates_stoptimes_stops=trips_stops,
    #     current_trips_filename=current_schedule_filename,
    # )

    # load_schedule_to_gcs(
    #     wait_for=[trips_today],
    #     prefect_gcs_block_name=prefect_gcs_block_name,
    #     from_path=f"{current_schedule_filename}.parquet.gzip",
    #     to_path=f"current_schedule/{current_schedule_filename}.parquet.gzip",
    # )

    return None


if __name__ == "__main__":
    get_gtfs_subway_schedule()
