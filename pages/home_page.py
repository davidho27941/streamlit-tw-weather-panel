import streamlit as st

single_location_single_date = st.Page(
    "single_location_single_date.py",
    title="Demo - Single locaiton",
    icon=":material/search:",
)
past_24_hour = st.Page(
    "past_3_hour.py", title="Demo - Latest 3 Hours", icon=":material/clock_loader_20:"
)


page = st.navigation(
    [
        single_location_single_date,
        past_24_hour,
    ]
)

page.run()
