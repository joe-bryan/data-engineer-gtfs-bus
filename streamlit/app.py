import streamlit as st
from google.cloud import storage
from google.oauth2 import service_account
import folium
from streamlit_folium import st_folium
from datetime import datetime
import pytz
import pandas as pd
from io import StringIO


def refresh_map():
    """Refreshes app data"""

    eastern_tz = pytz.timezone("US/Eastern")
    now = datetime.now(eastern_tz).strftime("%Y-%m-%d %H:%M:%S")

    # Create API client for gcs bucket
    credentials = service_account.Credentials.from_service_account_info(
        st.secrets.connections_gcs
    )

    client = storage.Client(credentials=credentials)

    def read_csvfile(bucket_name: str, file_path: str):
        """Retrieve GCS bucket content"""

        bucket = client.bucket(bucket_name)
        content = bucket.blob(file_path).download_as_string().decode("utf-8")

        return content

    def access_dataframe_from_gcsbucket():
        """Reads the GCS bucket csv file and converts to a Pandas DataFrame"""

        bucket_name = "subway-mbta-location"
        file_path = "late_subways_gold.csv"

        data = read_csvfile(bucket_name, file_path)
        dataframe = pd.read_csv(StringIO(data))

        return dataframe

    map = folium.Map(
        location=[
            42.3601,
            -71.0588,
        ],
        tiles="cartodbpositron",
        zoom_start=11.25,
    )

    def create_marker(
        lat, long, route, stop_name, late_by, scheduled_time, actual_time
    ):
        """Add circle markers to the map"""

        marker = folium.Circle(
            location=[lat, long],
            tooltip=f"<b>Route: {route} </b><br>Stop: {stop_name} <br>Late By: {late_by} <br>Scheduled: {scheduled_time} <br>Actual: {actual_time}",
            radius=100,
            color="red",
            fill=True,
        ).add_to(map)

        return marker

    try:
        # Uncomment this when going live to read csv file from GCP
        dataframe = access_dataframe_from_gcsbucket()

        dataframe["late_by"] = dataframe["late_by"].round(2)

        # number_of_late_subways = dataframe["trip_id"].count()

        for idx, row in dataframe.iterrows():
            lat = row["stop_lat"]
            long = row["stop_lon"]
            stop_name = row["stop_name"]
            late_by = row["late by"]
            route = row["route_long_name"] + " - " + row["trip_headsign"]
            scheduled_time = row["arrival_time_fixed"].split("+", 1)[0]
            actual_time = row["timestamp"].split("+", 1)[0]

            try:
                marker = create_marker(
                    lat, long, route, stop_name, late_by, scheduled_time, actual_time
                )
                marker.add_to(map)
            except:
                print("adding marker failed")
    except Exception as error:
        print(error)
        # number_of_late_subways = 0

    st.title("🚇MBTA Delayed Subways")
    # st.subheader(
    #     f"Number of subways currently late: {number_of_late_subways}",
    # )
    st.write(
        f"[Project GitHub](https://github.com/joe-bryan/data-engineer-gtfs-bus) | [Source data](https://www.mbta.com/schedules/subway) "
    )

    st_data = st_folium(map, width=800, height=500, returned_objects=[])

    st.text("Updates every 2 minutes")
    st.text(f"Last refreshed: {now} US Eastern Time")


st.button("Refresh", on_click=refresh_map())
