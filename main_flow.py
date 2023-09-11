from subway_locations import flow_live_locations_subway
from subway_locations_schedules import subway_times
from write_bigquery_table import write_subways_to_bigquery
from late_subway_gold import gold_flow
from prefect import flow


@flow
def main_flow():
    """Flow that encompasses four other Prefect flows"""

    # Get the live subway data
    live = flow_live_locations_subway()

    # Compare live subways with schedules
    late = subway_times(wait_for=live)

    # Write any late subways to bigquery table
    write_subways_to_bigquery(wait_for=late)

    # Create gold table for data visualization
    gold_flow(wait_for=write_subways_to_bigquery)


if __name__ == "__main__":
    main_flow()
