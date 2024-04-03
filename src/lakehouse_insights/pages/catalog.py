import json

import streamlit as st
import pandas as pd

st.set_page_config(
    page_title=" Data Lakehouse Catalog",
    page_icon="👋",
)

st.header("Data Lakehouse Catalog")

uploaded_catalog_file = st.file_uploader("Please add your Catalog config file:", type="json")

if uploaded_catalog_file is not None:
    input_data = json.load(uploaded_catalog_file)
    st.caption("Data Catalog loaded:")
    st.session_state['data_catalog'] = input_data
    st.json(st.session_state.data_catalog)
else:
    st.warning("Please upload a JSON file for the Catalog.")
