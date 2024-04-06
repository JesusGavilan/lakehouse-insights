from lakehouse_insights.utils.insights import get_insights_dataframe
import streamlit as st
import streamlit_shadcn_ui as ui

insights = get_insights_dataframe(st.session_state.data_catalog)
insights_grouped = zip(*(iter(insights),) * len(insights))
for insight in insights_grouped:
    cols = st.columns(3)
    with cols[0]:
        ui.metric_card(title=insight[0]['name'].values[0], content=f"{insight[0]['number_of_files'].values[0]} files",
                       description=f"{insight[0]['size_in_MB'].values[0]} MB", key="card1")
    with cols[1]:
        ui.metric_card(title=insight[1]['name'].values[0], content=f"{insight[1]['number_of_files'].values[0]} files",
                       description=f"{insight[1]['size_in_MB'].values[0]} MB", key="card2")
    with cols[2]:
        ui.metric_card(title=insight[2]['name'].values[0], content=f"{insight[2]['number_of_files'].values[0]} files",
                       description=f"{insight[2]['size_in_MB'].values[0]} MB", key="card3")
