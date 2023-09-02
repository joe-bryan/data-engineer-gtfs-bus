import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import sys
from io import StringIO

# Import the Secret Manager client library
from google.cloud import secretmanager
from google.cloud import storage

from dash import html, dcc, Input, Output, Dash, callback
import dash_bootstrap_components as dbc

# GCP project in which to store secrets in Secret Manager.
project_id = "1046417978179"

# Build the parent name from the project.
parent = f"projects/{project_id}"

# ID of the secret to create.
secret_id = "MAPBOX_ACCESS"

# Create the Secret Manager client.
client_1 = secretmanager.SecretManagerServiceClient()

# Build the parent name from the project.
parent = f"projects/{project_id}"

# Build the complete resource name of the secret version.
secret_resource_name = f"{parent}/secrets/{secret_id}/versions/1"

# Access the secret version.
response = client_1.access_secret_version(name=secret_resource_name)

# Get the secret value.
payload = response.payload.data.decode("UTF-8")


client_2 = storage.Client()


def read_csvfile(bucket_name: str, file_path: str):
    """Retrieve GCS bucket content"""

    bucket = client_2.bucket(bucket_name)
    content = bucket.blob(file_path).download_as_string().decode("utf-8")

    return content


def access_dataframe_from_gcsbucket():
    """Reads the GCS bucket csv file and converts to a Pandas DataFrame"""

    bucket_name = "subway-mbta-location"
    file_path = "late_subways.csv"

    data = read_csvfile(bucket_name, file_path)
    dataframe = pd.read_csv(StringIO(data))

    return dataframe


def subwaydata(dataframe: pd.DataFrame):
    """Test to make sure dataframe is read"""
    try:
        dataframe = dataframe
    except:
        sys.exit("Unable to load data file")

    return dataframe


def mean_latitude_longitude(dataframe: pd.DataFrame):
    """Find mean latitude and longitude to make the center point of the map"""
    center_latitude, center_longitude = (
        dataframe["latitude"].mean().item(),
        dataframe["longitude"].mean().item(),
    )

    return center_latitude, center_longitude


def transform_dataframe(
    dataframe: pd.DataFrame,
    column1: str,
    column2: str,
    column3: str,
    column4: str,
    column5: str,
):
    """Make changes to the dataframe to improve the visuals of the map"""
    dataframe[column1] = dataframe[column1].apply(
        lambda x: pd.to_datetime(x, format=("%H:%M:%S"))
    )

    dataframe[column1] = dataframe[column1].apply(lambda x: x.strftime("%H:%M %p"))

    dataframe[column2] = dataframe[column2].apply(
        lambda x: pd.to_datetime(x, format="%Y-%m-%d %H:%M:%S.%f")
    )

    dataframe[column3] = dataframe[column3].apply(
        lambda x: pd.to_datetime(x, format="%Y-%m-%d %H:%M:%S.%f")
    )

    dataframe[column4] = dataframe[column3] - dataframe[column2]

    dataframe[column4] = (
        dataframe[column4]
        .dt.total_seconds()
        .apply(lambda s: f"{(s % 3600) // 60:02.0f}:{s % 60:02.0f}")
    )

    dataframe[column5] = dataframe[column4].astype(str)

    dataframe[column5] = (
        dataframe[column5].str[:2]
        + " minutes and "
        + dataframe[column5].str[3:]
        + " seconds"
    )

    return dataframe


csv_data = access_dataframe_from_gcsbucket()
data = subwaydata(dataframe=csv_data)
lat, long = mean_latitude_longitude(dataframe=data)
subway_dataframe = transform_dataframe(
    dataframe=data,
    column1="arrival_time",
    column2="arrival_time_fixed",
    column3="timestamp",
    column4="late _",
    column5="late by",
)

px.set_mapbox_access_token(payload)

# create the dash application using the above layout definition
app = Dash(external_stylesheets=[dbc.themes.BOOTSTRAP])


def indicator():
    """Create an indicator to count the number of late subways"""
    CHART_THEME = "plotly_white"

    indicators = go.Figure()
    indicators.layout.template = CHART_THEME
    indicators.add_trace(
        go.Indicator(
            mode="number",
            value=len(subway_dataframe.axes[0]),
            number={"font": {"size": 60}},
            domain={"row": 0, "column": 0},
        )
    )

    return indicators


rows = html.Div(
    [
        dbc.Row(
            dbc.Col(
                html.Div(
                    [
                        html.H3(id="title", children="MBTA Subways App"),
                        html.H5(
                            id="subtitle",
                            children="Discover if your subway is late",
                        ),
                    ],
                    style={"textAlign": "center", "marginTop": 40, "marginBottom": 40},
                )
            )
        ),
        dbc.Row(
            [
                dbc.Col(
                    html.Div(
                        id="div-sidebar",
                        children=[
                            dbc.Card(
                                dbc.CardBody(
                                    [
                                        html.H6(
                                            children="Number of Late Subways",
                                            className="text-center",
                                        ),
                                        dcc.Graph(
                                            id="indicators",
                                            figure=indicator(),
                                            style={"height": 100},
                                        ),
                                    ]
                                )
                            ),
                            dbc.Card(
                                dbc.CardBody(
                                    [
                                        html.H6(
                                            children="Subway Lines",
                                            className="text-center",
                                        ),
                                        dbc.Checklist(
                                            options=[
                                                {"label": x, "value": x}
                                                for x in subway_dataframe[
                                                    "route_id"
                                                ].unique()
                                            ],
                                            value=subway_dataframe["route_id"].unique(),
                                            id="type-check-list",
                                        ),
                                    ]
                                )
                            ),
                        ],
                        style={"marginLeft": 20, "marginRight": 20},
                    ),
                    width=3,
                ),
                dbc.Col(
                    html.Div(
                        id="div-body",
                        children=[
                            dcc.Graph(id="mapbox"),
                            dcc.Interval(
                                id="interval-component",
                                interval=1 * 120000,
                                n_intervals=0,
                            ),
                        ],
                    ),
                    width=9,
                ),
            ]
        ),
    ]
)

app.layout = dbc.Container(rows, fluid=True)


@callback(
    Output(component_id="mapbox", component_property="figure"),
    Input(component_id="type-check-list", component_property="value"),
    Input(component_id="interval-component", component_property="n_intervals"),
)
def plot_late_subway_location(selected_route, n):
    """Create plotly map and have data points change when checklist changes"""
    plotly_subway_dataframe = subway_dataframe[
        subway_dataframe["route_id"].isin(selected_route)
    ]

    fig = px.scatter_mapbox(
        plotly_subway_dataframe,
        lat=plotly_subway_dataframe["latitude"],
        lon=plotly_subway_dataframe["longitude"],
        color="route_id",
        zoom=9.5,
        height=450,
        width=800,
        hover_name="route_id",
        hover_data={
            "route_id": False,
            "latitude": False,
            "longitude": False,
            "late_by": False,
            "arrival_time": True,
            "late by": True,
        },
        title=None,
        opacity=None,
        size="late_by",
        size_max=20,
        center={"lat": lat, "lon": long},
    )
    fig.update_layout(mapbox_style="dark", mapbox_accesstoken=payload)
    fig.update_layout(showlegend=False)
    fig["layout"]["uirevision"] = "unchanged"

    return fig


""" start the web application
    the host IP 0.0.0.0 is needed for dockerized version of this dash application
"""
if __name__ == "__main__":
    app.run_server(debug=False, host="0.0.0.0", port=8050)
    server = app.server
