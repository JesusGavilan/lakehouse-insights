import streamlit as st
import streamlit_shadcn_ui as ui
from lakehouse_insights.utils.insights import get_insights_dataframe

insights = get_insights_dataframe(st.session_state.data_catalog)
for insight in insights:
    print(insight)

