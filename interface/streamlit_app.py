import streamlit as st


pg = st.navigation(
    [
        st.Page("lang2sql.py", title="Lang2SQL"),
        st.Page("viz_eval.py", title="Lang2SQL Evaluation 시각화"),
    ]
)

pg.run()
