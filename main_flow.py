from subway_locations import flow_live_locations_subway
from subway_locations_schedules import subway_times
from write_bigquery_table import write_subways_to_bigquery
from prefect import flow


@flow
def main_flow():
    """Flow that encompasses three other Prefect flows"""

    # Get the live subway data
    live_location_subways = flow_live_locations_subway()

    # Compare live subways with schedules
    subway_times(wait_for=[live_location_subways])

    # Write any late subways to bigquery table
    write_subways_to_bigquery(wait_for=[subway_times])


if __name__ == "__main__":
    main_flow()