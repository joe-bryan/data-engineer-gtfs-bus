from datetime import datetime
import pandas as pd
import pytz
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from pathlib import Path
import os

# Set the timezone
tz = pytz.timezone("US/Eastern")

# Get the current timestamp
now = datetime.now(tz)

# Reset the time to have a clean datetime
dt = now.replace(hour=0, minute=0, second=0, microsecond=0)


@task(log_prints=True)
def schedule_from_gcs(
    current_schedule_filename: str, prefect_gcs_block_name: str
) -> Path:
    """Retrieve current schedule from Google Cloud Storage bucket"""

    gcs_path = f"current_schedule/{current_schedule_filename}.parquet.gzip"
    gcs_block = GcsBucket.load(prefect_gcs_block_name)
    # Download schedule to cwd
    gcs_block.get_directory(from_path=gcs_path)

    return Path(gcs_path)


@task(log_prints=True)
def subway_live_locations_from_gcs(
    live_locations_filename: str, prefect_gcs_block_name: str
) -> Path:
    """Retrieve live locations from Google Cloud Storage bucket"""

    gcs_path = f"live_location/{live_locations_filename}.parquet.gzip"
    gcs_block = GcsBucket.load(prefect_gcs_block_name)
    # Download live locations to cwd
    gcs_block.get_directory(from_path=gcs_path)

    return Path(gcs_path)


@task()
def combine_live_trips_with_schedule(
    trips_today: pd.DataFrame, live_locations: pd.DataFrame
) -> pd.DataFrame:
    """Merge all scheduled trips with live trip data"""

    compare = trips_today.merge(
        live_locations,
        left_on=["trip_id", "direction_id", "route_id"],
        right_on=["trip_id", "direction_id", "live_route_id"],
    )

    return compare


@task()
def calculate_subway_lateness(compare: pd.DataFrame) -> pd.DataFrame:
    """Calculate difference between subway scheduled time and actual live time"""

    # Resolve times that flow over to next day (e.g., 26:00 hours)
    compare.loc[:, "arrival_time_fixed"] = dt + pd.to_timedelta(compare["arrival_time"])
    compare.loc[:, "departure_time_fixed"] = dt + pd.to_timedelta(
        compare["departure_time"]
    )

    # Compare current time at stop with expected arrival time
    compare["arrival_time_fixed"] = pd.to_datetime(
        compare["arrival_time_fixed"], utc=True
    )

    # Convert arrival_time & departure_time to UTC timezone
    compare["arrival_time_fixed"] = compare["arrival_time_fixed"].dt.tz_convert(
        tz="US/Eastern"
    )

    # Create column that calculates how late subways are
    # if value is negative that means it is on the way to the
    # first stop
    compare["late_by"] = (
        compare["timestamp"] - compare["arrival_time_fixed"]
    ) / pd.Timedelta(minutes=1)

    # Remove timestamps that are not within the last 90 mins
    late_subways = compare[(now - compare["timestamp"]) / pd.Timedelta(minutes=1) <= 90]

    # Get subways later than _ minutes and less than 30 minutes at specific stop and remove current_status == 1
    late_subways_2 = late_subways[
        (late_subways["late_by"] > 3) & (late_subways["late_by"] < 30)
    ]

    # Only include trains that are not headed to the first stop
    late_subways_3 = late_subways_2[late_subways_2["stop_sequence"] != 1]

    return late_subways_3


@task()
def load_late_subways_to_gcs(
    late_subways_path: Path, prefect_gcs_block_name: str
) -> None:
    """Upload late subways to GCS"""

    gcs_block = GcsBucket.load(prefect_gcs_block_name)
    gcs_block.upload_from_path(from_path=late_subways_path)

    return None


@flow()
def subway_times(
    current_schedule_filename: str = "schedule_today",
    live_locations_filename: str = "live_location_subway",
    prefect_gcs_block_name: str = "subway-gcs-bucket",
):
    trips_today_path = schedule_from_gcs(
        current_schedule_filename, prefect_gcs_block_name
    )
    trips_today = pd.read_parquet(trips_today_path)

    live_locations_path = subway_live_locations_from_gcs(
        live_locations_filename, prefect_gcs_block_name
    )
    live_locations = pd.read_parquet(live_locations_path)

    compare = combine_live_trips_with_schedule(
        wait_for=[trips_today, live_locations],
        trips_today=trips_today,
        live_locations=live_locations,
    )

    os.remove(trips_today_path)
    os.remove(live_locations_path)
    os.rmdir("current_schedule")
    os.rmdir("live_location")

    late_subways = calculate_subway_lateness(wait_for=[compare], compare=compare)
    late_subways.to_csv("late_subways.csv", index=False)

    load_late_subways_to_gcs(
        wait_for=[late_subways],
        late_subways_path="late_subways.csv",
        prefect_gcs_block_name=prefect_gcs_block_name,
    )

    os.remove("late_subways.csv")


if __name__ == "__main__":
    subway_times()
