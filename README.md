# Smart Insights

Analyze messy CSV/Excel files with any schema, infer context (date/region/product/revenue/royalties), unify, generate insights & **charts**, and project the next 3 months. Includes a **Streamlit** front-end.

## Install (Windows)
```powershell
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

## Command-line usage
```powershell
python smart_insights.py --in "C:\path\file1.csv" "C:\path\file2.xlsx" --out "C:\path\out"
```

## Streamlit app
```powershell
streamlit run streamlit_app.py
```
Then open the local URL (usually http://localhost:8501), upload CSV/XLSX files, and click **Analyze**.

### Outputs
- `merged_canonical.csv`
- `role_mappings.json`
- `insights_report.xlsx` (contains a **Charts** sheet with the generated PNGs)
- `summary.md`
- `charts/` folder with all PNGs
