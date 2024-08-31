import streamlit as st


pg = st.navigation([
    st.Page("success_task_page.py", title="运行情况", icon="🔥"),
    # st.Page("compass_rate_view.py", title="压缩率查看")
])
pg.run()