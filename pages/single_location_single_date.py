import os
import json
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

import snowflake.connector

from datetime import datetime, timedelta


st.set_page_config(layout="wide", page_title="Taiwan Weather Panel")

USER = os.getenv("SF_USER")
PASSWORD = os.getenv("SF_PASSWD")
ACCOUNT = os.getenv("SF_ACCOUNT")
WAREHOUSE = os.getenv("SF_WHAREHOUSE")
DATABASE = os.getenv("SF_DATABASE")

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
            STATIONID,
            STATIONNAME,
            MIN(OBSTIME),
            MAX(OBSTIME)
        FROM cwb_dev_transformed.weather_records
        GROUP BY STATIONID, STATIONNAME
        """
    )

    station_table = pd.DataFrame(
        cursor.fetchall(), columns=["stationID", "stationName", "startDate", "endDate"]
    )

    cursor.execute(
        """
        SELECT 
            DISTINCT STATIONID,
            STATIONNAME,
            COUNTYNAME,
            COUNTYCODE,
            COORDINATES_TWD67:StationLatitude AS Lat,
            COORDINATES_TWD67:StationLongitude AS Lon
        FROM  cwb_dev_transformed.geoinfo
        WHERE STARTSWITH(STATIONID , 46)
        """
    )

    station_geo_table = pd.DataFrame(
        cursor.fetchall(), columns=[
            "stationID", 
            "stationName", 
            "countryName", 
            "countryCode", 
            "startDate", 
            "endDate"
        ]
    )


st.title("Taiwan Weather Panel")
st.markdown(
    """
    On this page, we will allow users to select a weather station and a date, and provide temperature information for the selected date within 24 hours. The results will be displayed in three tabs: one with a temperature vs. time line chart, one with the raw data table, and one with the geographical information of the weather station.
    """
)

with st.container(border=True):
    
    col1, col2 = st.columns(2)
    
    with col1:
        grouped = station_geo_table.groupby('countryName')
        
        country_city_option = st.selectbox(
            "Please select a Country/City:",
            options=list(grouped.groups.keys()),
            index=0,
        )
    
    with col2:
        station_name = st.selectbox(
            "Please select a station:",
            options=grouped.get_group(country_city_option).stationName.to_list(),
            index=0,
        )

    selected_sataion_table = station_table[station_table["stationName"] == station_name]

    start_date = str(selected_sataion_table["startDate"].to_list()[0])
    end_date = str(selected_sataion_table["endDate"].to_list()[0])

    selected_date = st.date_input(
        "Select a start date:",
        min_value=datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S"),
        max_value=datetime.strptime(end_date, "%Y-%m-%d %H:%M:%S"),
        value=datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S"),
    )

    clicked = st.button("Submit", type="primary", use_container_width=True)

    end_date = selected_date + timedelta(hours=24)
    end_date = f"{end_date:%Y-%m-%d}"
    selected_date = f"{selected_date:%Y-%m-%d}"


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
            f"""
            SELECT 
                * 
            FROM cwb_dev_transformed.weather_records 
            WHERE STATIONNAME = '{station_name}'
                AND OBSTIME between '{selected_date}' AND '{end_date}'
            ORDER BY OBSTIME
            """
        )

        weather_df = pd.DataFrame(
            cursor.fetchall(),
            columns=[
                "Station ID",
                "Station Name",
                "Time",
                "weather",
                "temperature",
                "pressure",
                "humidity",
                "wind speed",
                "wind direction",
                "wind deriection GUST",
                "peak GUST speed",
                "preciptation",
                "sunshine duration in 10 minutes",
                "visibility",
                "UV index",
            ],
        )

        weather_df = weather_df.astype(
            {
                "Station ID": "string",
                "Station Name": "string",
                "Time": "datetime64[ms]",
                "weather": "string",
                "temperature": "float",
                "pressure": "float",
                "humidity": "float",
                "wind speed": float,
                "wind direction": int,
                "wind deriection GUST": int,
                "peak GUST speed": float,
                "preciptation": float,
                "sunshine duration in 10 minutes": "string",
                "visibility": "string",
                "UV index": "string",
            }
        )

    with snowflake.connector.connect(
        user=USER,
        password=PASSWORD,
        account=ACCOUNT,
        warehouse=WAREHOUSE,
        database=DATABASE,
    ) as con:

        cursor = con.cursor()

        cursor.execute(
            f"""
            SELECT 
                COORDINATES_TWD67 
            FROM WEATHER_DATA.CWB_DEV_TRANSFORMED.GEOINFO 
            WHERE STATIONNAME = '{station_name}'
            LIMIT 1
            """
        )
        geo_info = json.loads(cursor.fetchall()[0][0])
        geo_df = pd.DataFrame(
            {
                "lat": geo_info["StationLatitude"],
                "lon": geo_info["StationLongitude"],
            },
            index=[0],
        )

    with st.container(border=True, height=600):
        tab1, tab2, tab3 = st.tabs(["Temperature", "location", "Weather Table"])

        with tab3:
            st.dataframe(weather_df, use_container_width=True, height=500)

        with tab1:

            temperature = px.line(weather_df, x="Time", y=["temperature"])
            temperature.update_layout(
                showlegend=False,
            )
            temperature.update_xaxes(title="Temperature")
            st.plotly_chart(temperature, use_container_width=True)

        with tab2:
            st.map(geo_df, latitude="lat", longitude="lon", use_container_width=True)
