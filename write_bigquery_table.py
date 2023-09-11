from prefect import flow, task
from prefect_gcp import GcpCredentials
from prefect_gcp.bigquery import bigquery_load_file
from google.cloud.bigquery import SchemaField
from pathlib import Path
from prefect_gcp.cloud_storage import GcsBucket
import os


@task(retries=3)
def subways_from_gcs(late_subways_filename: str, prefect_gcs_block_name: str) -> Path:
    """Retrieve late subways from bucket"""

    gcs_path = late_subways_filename
    gcs_block = GcsBucket.load(prefect_gcs_block_name)
    gcs_block.get_directory(from_path=gcs_path)

    return Path(gcs_path)


@flow
def write_subways_to_bigquery():
    gcp_project_id = "subway-mbta"
    gcp_credentials = GcpCredentials.load("subway-credentials")

    prefect_gcs_block_name = "subway-gcs-bucket"
    late_subways_filename = "late_subways.csv"

    late_subways_path = subways_from_gcs(
        late_subways_filename=late_subways_filename,
        prefect_gcs_block_name=prefect_gcs_block_name,
    )

    schema = [
        SchemaField("route_id", field_type="STRING", mode="REQUIRED"),
        SchemaField("service_id", field_type="STRING", mode="REQUIRED"),
        SchemaField("trip_id", field_type="STRING", mode="REQUIRED"),
        SchemaField("trip_headsign", field_type="STRING", mode="REQUIRED"),
        SchemaField("direction_id", field_type="STRING", mode="REQUIRED"),
        SchemaField("wheelchair_accessible", field_type="STRING", mode="NULLABLE"),
        SchemaField("route_pattern_id", field_type="STRING", mode="NULLABLE"),
        SchemaField("bikes_allowed", field_type="STRING", mode="NULLABLE"),
        SchemaField("agency_id", field_type="STRING", mode="REQUIRED"),
        SchemaField("route_short_name", field_type="STRING", mode="NULLABLE"),
        SchemaField("route_long_name", field_type="STRING", mode="NULLABLE"),
        SchemaField("route_desc", field_type="STRING", mode="NULLABLE"),
        SchemaField("route_type", field_type="STRING", mode="NULLABLE"),
        SchemaField("route_url", field_type="STRING", mode="NULLABLE"),
        SchemaField("route_fare_class", field_type="STRING", mode="NULLABLE"),
        SchemaField("line_id", field_type="STRING", mode="NULLABLE"),
        SchemaField("network_id", field_type="STRING", mode="NULLABLE"),
        SchemaField("monday", field_type="STRING", mode="NULLABLE"),
        SchemaField("tuesday", field_type="STRING", mode="NULLABLE"),
        SchemaField("wednesday", field_type="STRING", mode="NULLABLE"),
        SchemaField("thursday", field_type="STRING", mode="NULLABLE"),
        SchemaField("friday", field_type="STRING", mode="NULLABLE"),
        SchemaField("saturday", field_type="STRING", mode="NULLABLE"),
        SchemaField("sunday", field_type="STRING", mode="NULLABLE"),
        SchemaField("start_date", field_type="DATE", mode="REQUIRED"),
        SchemaField("end_date", field_type="DATE", mode="REQUIRED"),
        SchemaField("arrival_time", field_type="TIME", mode="REQUIRED"),
        SchemaField("departure_time", field_type="TIME", mode="REQUIRED"),
        SchemaField("stop_id", field_type="STRING", mode="REQUIRED"),
        SchemaField("stop_sequence", field_type="STRING", mode="NULLABLE"),
        SchemaField("stop_name", field_type="STRING", mode="NULLABLE"),
        SchemaField("stop_desc", field_type="STRING", mode="NULLABLE"),
        SchemaField("stop_lat", field_type="FLOAT64", mode="REQUIRED"),
        SchemaField("stop_lon", field_type="FLOAT64", mode="REQUIRED"),
        SchemaField("zone_id", field_type="STRING", mode="NULLABLE"),
        SchemaField("id", field_type="STRING", mode="REQUIRED"),
        SchemaField("start_time", field_type="TIME", mode="REQUIRED"),
        SchemaField("live_start_date", field_type="DATE", mode="REQUIRED"),
        SchemaField("schedule_relationship", field_type="STRING", mode="NULLABLE"),
        SchemaField("live_route_id", field_type="STRING", mode="REQUIRED"),
        SchemaField("latitude", field_type="FLOAT64", mode="NULLABLE"),
        SchemaField("longitude", field_type="FLOAT64", mode="NULLABLE"),
        SchemaField("bearing", field_type="FLOAT64", mode="NULLABLE"),
        SchemaField("speed", field_type="FLOAT64", mode="NULLABLE"),
        SchemaField("current_stop", field_type="STRING", mode="NULLABLE"),
        SchemaField("current_status", field_type="STRING", mode="NULLABLE"),
        SchemaField("timestamp", field_type="STRING", mode="REQUIRED"),
        SchemaField("live_stop_id", field_type="STRING", mode="NULLABLE"),
        SchemaField("vehicle", field_type="STRING", mode="NULLABLE"),
        SchemaField("label", field_type="STRING", mode="NULLABLE"),
        SchemaField("arrival_time_fixed", field_type="STRING", mode="REQUIRED"),
        SchemaField("departure_time_fixed", field_type="STRING", mode="REQUIRED"),
        SchemaField("late_by", field_type="FLOAT64", mode="REQUIRED"),
    ]

    result = bigquery_load_file(
        dataset="subway_mbta",
        table="raw_subway_mbta",
        path=late_subways_path,
        schema=schema,
        gcp_credentials=gcp_credentials,
        project=gcp_project_id,
    )

    os.remove("late_subways.csv")

    return result


if __name__ == "__main__":
    write_subways_to_bigquery()
