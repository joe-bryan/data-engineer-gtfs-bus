from prefect_gcp.bigquery import bigquery_create_table
from google.cloud.bigquery import SchemaField
from prefect_gcp import GcpCredentials
from prefect import flow


@flow
def create_biqquery_table():
    gcp_credentials = GcpCredentials.load("subway-credentials")

    schema = [
        SchemaField("route_id", field_type="STRING", mode="REQUIRED"),
        SchemaField("service_id", field_type="STRING", mode="REQUIRED"),
        SchemaField("trip_id", field_type="STRING", mode="REQUIRED"),
        SchemaField("trip_headsign", field_type="STRING", mode="REQUIRED"),
        SchemaField("direction_id", field_type="STRING", mode="REQUIRED"),
        SchemaField("block_id", field_type="STRING", mode="REQUIRED"),
        SchemaField("shape_id", field_type="STRING", mode="REQUIRED"),
        SchemaField("wheelchair_accessible", field_type="STRING", mode="REQUIRED"),
        SchemaField("route_pattern_id", field_type="STRING", mode="REQUIRED"),
        SchemaField("bikes_allowed", field_type="STRING", mode="REQUIRED"),
        SchemaField("agency_id", field_type="STRING", mode="REQUIRED"),
        SchemaField("route_short_name", field_type="STRING", mode="NULLABLE"),
        SchemaField("route_long_name", field_type="STRING", mode="REQUIRED"),
        SchemaField("route_desc", field_type="STRING", mode="REQUIRED"),
        SchemaField("route_type", field_type="STRING", mode="REQUIRED"),
        SchemaField("route_url", field_type="STRING", mode="REQUIRED"),
        SchemaField("route_color", field_type="STRING", mode="REQUIRED"),
        SchemaField("route_text_color", field_type="STRING", mode="REQUIRED"),
        SchemaField("route_sort_order", field_type="STRING", mode="REQUIRED"),
        SchemaField("route_fare_class", field_type="STRING", mode="REQUIRED"),
        SchemaField("line_id", field_type="STRING", mode="REQUIRED"),
        SchemaField("network_id", field_type="STRING", mode="REQUIRED"),
        SchemaField("monday", field_type="STRING", mode="REQUIRED"),
        SchemaField("tuesday", field_type="STRING", mode="REQUIRED"),
        SchemaField("wednesday", field_type="STRING", mode="REQUIRED"),
        SchemaField("thursday", field_type="STRING", mode="REQUIRED"),
        SchemaField("friday", field_type="STRING", mode="REQUIRED"),
        SchemaField("saturday", field_type="STRING", mode="REQUIRED"),
        SchemaField("sunday", field_type="STRING", mode="REQUIRED"),
        SchemaField("start_date", field_type="DATE", mode="REQUIRED"),
        SchemaField("end_date", field_type="DATE", mode="REQUIRED"),
        SchemaField("arrival_time", field_type="TIME", mode="REQUIRED"),
        SchemaField("departure_time", field_type="TIME", mode="REQUIRED"),
        SchemaField("stop_id", field_type="STRING", mode="REQUIRED"),
        SchemaField("stop_sequence", field_type="STRING", mode="REQUIRED"),
        SchemaField("pickup_type", field_type="STRING", mode="NULLABLE"),
        SchemaField("drop_off_type", field_type="STRING", mode="NULLABLE"),
        SchemaField("timepoint", field_type="STRING", mode="REQUIRED"),
        SchemaField("checkpoint_id", field_type="STRING", mode="NULLABLE"),
        SchemaField("stop_code", field_type="STRING", mode="NULLABLE"),
        SchemaField("stop_name", field_type="STRING", mode="NULLABLE"),
        SchemaField("stop_desc", field_type="STRING", mode="NULLABLE"),
        SchemaField("platform_code", field_type="STRING", mode="NULLABLE"),
        SchemaField("platform_name", field_type="STRING", mode="NULLABLE"),
        SchemaField("stop_lat", field_type="FLOAT64", mode="NULLABLE"),
        SchemaField("stop_lon", field_type="FLOAT64", mode="NULLABLE"),
        SchemaField("zone_id", field_type="STRING", mode="NULLABLE"),
        SchemaField("stop_url", field_type="STRING", mode="NULLABLE"),
        SchemaField("level_id", field_type="STRING", mode="NULLABLE"),
        SchemaField("location_type", field_type="STRING", mode="NULLABLE"),
        SchemaField("parent_station", field_type="STRING", mode="NULLABLE"),
        SchemaField("wheelchair_boarding", field_type="STRING", mode="NULLABLE"),
        SchemaField("municipality", field_type="STRING", mode="NULLABLE"),
        SchemaField("vehicle_type", field_type="STRING", mode="NULLABLE"),
        SchemaField("id", field_type="STRING", mode="REQUIRED"),
        SchemaField("start_time", field_type="TIME", mode="REQUIRED"),
        SchemaField("live_start_date", field_type="DATE", mode="REQUIRED"),
        SchemaField("schedule_relationship", field_type="STRING", mode="REQUIRED"),
        SchemaField("live_route_id", field_type="STRING", mode="REQUIRED"),
        SchemaField("latitude", field_type="FLOAT64", mode="REQUIRED"),
        SchemaField("longitude", field_type="FLOAT64", mode="REQUIRED"),
        SchemaField("bearing", field_type="FLOAT64", mode="REQUIRED"),
        SchemaField("speed", field_type="FLOAT64", mode="REQUIRED"),
        SchemaField("current_stop", field_type="STRING", mode="REQUIRED"),
        SchemaField("current_status", field_type="STRING", mode="REQUIRED"),
        SchemaField("timestamp", field_type="STRING", mode="REQUIRED"),
        SchemaField("vehicle", field_type="STRING", mode="REQUIRED"),
        SchemaField("label", field_type="STRING", mode="REQUIRED"),
        SchemaField("arrival_time_fixed", field_type="STRING", mode="REQUIRED"),
        SchemaField("departure_time_fixed", field_type="STRING", mode="REQUIRED"),
        SchemaField("late_by", field_type="FLOAT64", mode="REQUIRED"),
    ]

    bigquery_create_table(
        dataset="subway_mbta",
        table="raw_subway",
        schema=schema,
        gcp_credentials=gcp_credentials,
    )


if __name__ == "__main__":
    create_biqquery_table()
