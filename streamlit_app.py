
"""
streamlit_app.py — Drag-and-drop UI for Smart Insights.

Run:
    streamlit run streamlit_app.py
"""
import os
import io
import json
import tempfile
from datetime import datetime

import pandas as pd
import streamlit as st

from smart_insights import generate_report, load_file, infer_semantic_roles, unify_to_canonical

st.set_page_config(page_title="Smart Insights", layout="wide")
st.title("📊 Smart Insights — Schema-Agnostic Analytics")

st.markdown("Drop CSV/XLSX files with **any** layout. I'll infer meaning, unify them, and generate insights, projections, and charts.")

uploaded = st.file_uploader("Upload one or more CSV/XLSX files", type=["csv","xlsx","xls"], accept_multiple_files=True)

out_parent = tempfile.mkdtemp(prefix="smart_insights_")
out_dir = os.path.join(out_parent, "output")
os.makedirs(out_dir, exist_ok=True)

if st.button("Analyze", type="primary") and uploaded:
    # Save uploads to disk
    input_paths = []
    for uf in uploaded:
        p = os.path.join(out_parent, uf.name)
        with open(p, "wb") as f:
            f.write(uf.getbuffer())
        input_paths.append(p)

    # Run pipeline
    outputs = generate_report(input_paths, out_dir)

    st.success("Analysis complete.")
    st.write("**Outputs**")
    st.code(json.dumps(outputs, indent=2))

    # Preview role mappings
    with open(outputs["roles"], "r", encoding="utf-8") as f:
        roles = json.load(f)
    st.subheader("Role mappings")
    st.json(roles)

    # Show a few insights
    merged = pd.read_csv(outputs["merged"])
    st.subheader("Merged sample")
    st.dataframe(merged.head(20))

    charts_dir = outputs.get("charts_dir")
    if charts_dir and os.path.isdir(charts_dir):
        st.subheader("Charts")
        imgs = [os.path.join(charts_dir, x) for x in sorted(os.listdir(charts_dir)) if x.lower().endswith(".png")]
        cols = st.columns(2)
        for i, img in enumerate(imgs):
            cols[i % 2].image(img, use_column_width=True, caption=os.path.basename(img))

    # Build a zip to download
    zip_path = os.path.join(out_parent, "smart_insights_outputs.zip")
    import zipfile
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as z:
        for root, _, files in os.walk(out_dir):
            for fn in files:
                full = os.path.join(root, fn)
                arc = os.path.relpath(full, out_dir)
                z.write(full, arcname=arc)

    with open(zip_path, "rb") as f:
        st.download_button("Download all outputs (zip)", f, file_name="smart_insights_outputs.zip")
else:
    st.info("Upload files and click **Analyze**")
