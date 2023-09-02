import requests
import gtfs_kit as gk
import os
from prefect import flow, task


@task
def schedule_feed(schedule_url: str):
    """Get newest schedule GTFS file from Massachusets Bay Transportation Authority"""

    r = requests.get(schedule_url)
    with open("MBTA_GTFS.zip", "wb") as fd:
        for chunk in r.iter_content(chunk_size=128):
            fd.write(chunk)

    feed = gk.read_feed("MBTA_GTFS.zip", dist_units="mi")

    os.remove("MBTA_GTFS.zip")

    return feed


@flow
def subway_schedule(
    schedule_url: str = "https://cdn.mbta.com/MBTA_GTFS.zip",
) -> None:
    schedule_feed(schedule_url=schedule_url)

    return None


if __name__ == "__main__":
    subway_schedule()
