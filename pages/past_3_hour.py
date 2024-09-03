import os
import json
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import geopandas as gpd
import snowflake.connector

from datetime import datetime, timedelta

st.set_page_config(layout="wide", page_title="Taiwan Weather Panel")

USER = os.getenv("SF_USER")
PASSWORD = os.getenv("SF_PASSWD")
ACCOUNT = os.getenv("SF_ACCOUNT")
WAREHOUSE = os.getenv("SF_WHAREHOUSE")
DATABASE = os.getenv("SF_DATABASE")

st.title("Taiwan Weather Panel - Latest 3 Hour Data")
st.markdown(
    """
    On this page, we will display the temperature data for various counties and cities in Taiwan over the latest three hours, with a time interval of 10 minutes.
    """
)

st.warning(
    "The Yunlin station is currently under maintenance, so its data is unavailable."
)

with st.container():
    clicked = st.button("Generate!", type="primary")

if clicked:

    with snowflake.connector.connect(
        user=USER,
        password=PASSWORD,
        account=ACCOUNT,
        warehouse=WAREHOUSE,
        database=DATABASE,
    ) as con:
        cursor = con.cursor()

        cursor.execute(
            """
            SELECT 
                MAX(OBSTIME)
            FROM cwb_dev_transformed.weather_records
            """
        )

        latest_time = cursor.fetchall()[0][0]
        previous_3 = latest_time - timedelta(hours=3)

        cursor.execute(
            f"""
            SELECT 
                STATIONID,
                STATIONNAME,
                OBSTIME,
                AIRTEMPERATURE
            FROM cwb_dev_transformed.weather_records_v2
            WHERE OBSTIME BETWEEN '{previous_3:%Y-%m-%d %H:%M:%S}' AND '{latest_time:%Y-%m-%d %H:%M:%S}'
                AND STARTSWITH(STATIONID , 46)  AND STATIONID != 468100 AND STATIONID != 469020
            ORDER BY OBSTIME
            """
        )

        recent_weather = pd.DataFrame(
            cursor.fetchall(),
            columns=[
                "Station ID",
                "Station Name",
                "Time",
                "temperature",
            ],
        )

        cursor.execute(
            """
            SELECT 
                DISTINCT STATIONID,
                STATIONNAME,
                COUNTYCODE,
                COORDINATES_TWD67:StationLatitude AS Lat,
                COORDINATES_TWD67:StationLongitude AS Lon
            FROM  cwb_dev_transformed.geoinfo_v2
            WHERE STARTSWITH(STATIONID , 46)
            """
        )
        station_info = pd.DataFrame(
            cursor.fetchall(),
            columns=["Station ID", "Station Name", "country ID", "Lat", "Lon"],
        )

    with st.container():
        
        merged = recent_weather.merge(station_info, on="Station ID", how="inner")

        merged["temperature"] = merged["temperature"].astype("float")

        taiwan_geo = gpd.read_file(f"{os.getcwd()}/taiwan_geo_tw97/COUNTY_MOI_1130718.shp")
        
        taiwan_geo.geometry = taiwan_geo.geometry.simplify(0.01, preserve_topology=True)
        taiwan_geo_json = taiwan_geo.to_json()
        taiwan_geo_json = json.loads(taiwan_geo_json)

        fig = px.choropleth_mapbox(
            merged,
            geojson=taiwan_geo_json,
            locations=merged["country ID"],
            color="temperature",
            mapbox_style="carto-positron",
            featureidkey="properties.COUNTYCODE",
            zoom=5,
            center={"lat": 23.97, "lon": 120.97},
            animation_frame="Time",
            color_continuous_scale=px.colors.sequential.Rainbow,
            opacity=0.5,
            range_color=(0, 40),
        )
        # fig.update_coloraxes(colorbar_title="Your Value", colorscale='ylorrd', color_continuous_scale="log")

        st.plotly_chart(fig, use_container_width=True)
