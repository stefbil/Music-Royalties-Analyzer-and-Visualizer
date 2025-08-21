
"""
Usage (CLI, Windows):
    python smart_insights.py --in "C:\path\file1.csv" "C:\path\file2.xlsx" --out "C:\path\out"

Outputs:
    - merged_canonical.csv
    - role_mappings.json
    - insights_report.xlsx     (includes a Charts sheet)
    - summary.md
    - charts/ (PNG images)

Author: Stefbil
"""
import os
import re
import json
import argparse
from datetime import datetime
from typing import List, Dict, Tuple, Optional

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# ---------------- Utilities ----------------

def clean_col(s: str) -> str:
    s = str(s)
    s = s.strip().lower()
    s = re.sub(r"[\s\-\/]+", "_", s)
    s = re.sub(r"[^a-z0-9_]", "", s)
    s = re.sub(r"_+", "_", s)
    return s.strip("_")


def try_parse_dates(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce", infer_datetime_format=True)


def is_date_col(series: pd.Series) -> bool:
    dt = try_parse_dates(series)
    non_na = dt.notna().mean()
    return non_na > 0.5


def is_numeric_col(series: pd.Series) -> bool:
    numeric = pd.to_numeric(series, errors="coerce")
    return numeric.notna().mean() > 0.7


COUNTRIES = {
    "united states","usa","us","u.s.","u.s.a.",
    "united kingdom","uk","u.k.","england","scotland","wales","northern ireland",
    "canada","mexico","brazil","argentina","chile","peru","colombia",
    "germany","france","spain","italy","portugal","netherlands","belgium","sweden","norway","denmark","finland","poland","czech republic","austria","switzerland","ireland","greece","turkey","romania","hungary",
    "russia","ukraine",
    "china","india","japan","south korea","korea","singapore","malaysia","indonesia","philippines","thailand","vietnam","taiwan","hong kong",
    "australia","new zealand",
    "south africa","nigeria","egypt","kenya","morocco",
    "uae","united arab emirates","saudi arabia","israel","qatar"
}

REGION_WORDS = {"region","country","territory","market","state","province","city","area"}
PRODUCT_WORDS = {"product","sku","item","title","track","song","album","game","plan","service","category"}
REVENUE_WORDS = {"revenue","sales","amount","gross","net_sales","income","turnover"}
ROYALTY_WORDS = {"royalty","payout","commission"}
RATE_WORDS = {"royalty_rate","rate","rev_share","revshare","percentage","percent"}
UNITS_WORDS = {"units","qty","quantity","streams","plays","downloads","views"}
CURRENCY_WORDS = {"currency","ccy","curr"}


def infer_semantic_roles(df: pd.DataFrame) -> Dict[str, str]:
    roles = {}
    cols = list(df.columns)
    # dates first
    for c in cols:
        s = df[c]
        if c in roles.values():
            continue
        if is_date_col(s):
            roles.setdefault("date", c)

    def match_on_name(c: str, keywords: set) -> bool:
        return any(k in c for k in keywords)

    def best_numeric(candidates: List[str]) -> Optional[str]:
        best = None
        best_var = -1
        for c in candidates:
            vals = pd.to_numeric(df[c], errors="coerce")
            var = np.nanvar(vals.values)
            if np.isnan(var):
                var = -1
            if var > best_var:
                best_var = var
                best = c
        return best

    # Region
    region_candidates = []
    for c in cols:
        if c == roles.get("date"):
            continue
        if match_on_name(c, REGION_WORDS):
            region_candidates.append(c)
    if not region_candidates:
        for c in cols:
            if df[c].dtype == object:
                sample = df[c].astype(str).str.lower().head(200)
                matches = sample.apply(lambda x: x.strip().lower() in COUNTRIES)
                if matches.mean() > 0.15:
                    region_candidates.append(c)
    if region_candidates:
        roles.setdefault("region", region_candidates[0])

    # Product, currency
    product_candidates = [c for c in cols if match_on_name(c, PRODUCT_WORDS)]
    if product_candidates:
        roles.setdefault("product", product_candidates[0])

    currency_candidates = [c for c in cols if match_on_name(c, CURRENCY_WORDS)]
    if currency_candidates:
        roles.setdefault("currency", currency_candidates[0])

    # Numeric
    royalty_amount_candidates = [c for c in cols if match_on_name(c, ROYALTY_WORDS) and is_numeric_col(df[c])]
    if royalty_amount_candidates:
        roles.setdefault("royalty_amount", best_numeric(royalty_amount_candidates))

    rate_candidates = [c for c in cols if match_on_name(c, RATE_WORDS) and is_numeric_col(df[c])]
    if rate_candidates:
        roles.setdefault("royalty_rate", best_numeric(rate_candidates))

    revenue_candidates = [c for c in cols if match_on_name(c, REVENUE_WORDS) and is_numeric_col(df[c])]
    if revenue_candidates:
        roles.setdefault("revenue", best_numeric(revenue_candidates))

    units_candidates = [c for c in cols if match_on_name(c, UNITS_WORDS) and is_numeric_col(df[c])]
    if units_candidates:
        roles.setdefault("units", best_numeric(units_candidates))

    if "revenue" not in roles:
        numeric_cols = [c for c in cols if is_numeric_col(df[c])]
        if numeric_cols:
            roles["revenue"] = best_numeric(numeric_cols)

    return roles


def load_file(path: str) -> pd.DataFrame:
    ext = os.path.splitext(path)[1].lower()
    if ext in [".csv", ".txt"]:
        try:
            df = pd.read_csv(path)
        except Exception:
            try:
                df = pd.read_csv(path, sep=";")
            except Exception:
                df = pd.read_csv(path, sep="\t")
    elif ext in [".xls", ".xlsx"]:
        df = pd.read_excel(path)
    else:
        raise ValueError(f"Unsupported file type: {ext}")
    df = df.rename(columns={c: clean_col(c) for c in df.columns})
    return df


def unify_to_canonical(df: pd.DataFrame, roles: Dict[str, str], source_file: str) -> pd.DataFrame:
    out = pd.DataFrame()
    out["source_file"] = source_file
    # basic fields
    out["date"] = try_parse_dates(df[roles["date"]]) if "date" in roles else pd.NaT
    out["region"] = df[roles["region"]] if "region" in roles else np.nan
    out["product"] = df[roles["product"]] if "product" in roles else np.nan
    out["currency"] = df[roles["currency"]] if "currency" in roles else np.nan

    def safe_num(colname: Optional[str]) -> pd.Series:
        if not colname or colname not in df.columns:
            return pd.Series([np.nan] * len(df))
        return pd.to_numeric(df[colname], errors="coerce")

    out["revenue"] = safe_num(roles.get("revenue"))
    out["royalty_amount"] = safe_num(roles.get("royalty_amount"))
    out["royalty_rate"] = safe_num(roles.get("royalty_rate"))
    out["units"] = safe_num(roles.get("units"))

    # derive royalty_amount if missing
    if out["royalty_amount"].isna().all() and out["revenue"].notna().any() and out["royalty_rate"].notna().any():
        out["royalty_amount"] = out["revenue"] * (out["royalty_rate"] / 100.0 if out["royalty_rate"].max() > 1.0 else out["royalty_rate"])

    return out


def summarize_insights(merged: pd.DataFrame):
    result = {}
    df = merged.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["month"] = df["date"].dt.to_period("M").dt.to_timestamp()

    regional = df.groupby(["region", "month"], dropna=False)["revenue"].sum().reset_index()
    regional["revenue_prev"] = regional.groupby("region")["revenue"].shift(1)
    regional["mom_growth"] = (regional["revenue"] - regional["revenue_prev"]) / regional["revenue_prev"]
    result["regional_trends"] = regional.sort_values(["region", "month"])

    if "product" in df.columns:
        product = df.groupby(["product", "month"], dropna=False)["revenue"].sum().reset_index()
        product["revenue_prev"] = product.groupby("product")["revenue"].shift(1)
        product["mom_growth"] = (product["revenue"] - product["revenue_prev"]) / product["revenue_prev"]
        result["product_trends"] = product.sort_values(["product", "month"])

    royalties = df.groupby(["region", "month"], dropna=False)[["royalty_amount"]].sum().reset_index()
    result["royalties"] = royalties.sort_values(["region", "month"])

    overall = df.groupby("month", dropna=False)[["revenue", "royalty_amount", "units"]].sum().reset_index()
    result["overall"] = overall

    # Outliers (z-score by region)
    outlier_df = df.copy()
    outlier_df["revenue_mean_by_region"] = outlier_df.groupby("region")["revenue"].transform("mean")
    outlier_df["revenue_std_by_region"] = outlier_df.groupby("region")["revenue"].transform("std")
    outlier_df["revenue_z"] = (outlier_df["revenue"] - outlier_df["revenue_mean_by_region"]) / outlier_df["revenue_std_by_region"]
    outliers = outlier_df.loc[outlier_df["revenue_z"].abs() > 3, ["source_file","date","region","product","revenue","revenue_z"]]
    result["outliers"] = outliers

    return result


def simple_projection(series: pd.Series, periods_ahead: int = 3):
    y = pd.to_numeric(series, errors="coerce").astype(float).values
    idx = np.arange(len(y))
    mask = ~np.isnan(y)
    if mask.sum() < 2:
        last = y[mask][-1] if mask.any() else np.nan
        return np.arange(len(y), len(y)+periods_ahead), np.array([last]*periods_ahead, dtype=float)
    coef = np.polyfit(idx[mask], y[mask], 1)
    trend = np.poly1d(coef)
    x_future = np.arange(len(y), len(y) + periods_ahead)
    y_future = trend(x_future)
    return x_future, y_future


def build_projections(merged: pd.DataFrame) -> pd.DataFrame:
    df = merged.copy()
    df["month"] = pd.to_datetime(df["date"], errors="coerce").dt.to_period("M").dt.to_timestamp()
    grp = df.groupby("month")[["revenue","royalty_amount"]].sum().reset_index().sort_values("month")
    proj_rows = []
    for col in ["revenue","royalty_amount"]:
        x_future, y_future = simple_projection(grp[col], periods_ahead=3)
        if len(grp) > 0 and pd.notna(grp["month"].iloc[-1]):
            last_month = grp["month"].iloc[-1]
            future_months = pd.period_range(last_month.to_period("M")+1, periods=len(y_future), freq="M").to_timestamp()
        else:
            future_months = pd.date_range(datetime.today(), periods=len(y_future), freq="M")
        for m, yhat in zip(future_months, y_future):
            proj_rows.append({"month": m, "metric": col, "forecast": float(yhat)})
    return pd.DataFrame(proj_rows)


def save_charts(merged: pd.DataFrame, out_dir: str) -> Dict[str, str]:
    charts_dir = os.path.join(out_dir, "charts")
    os.makedirs(charts_dir, exist_ok=True)
    df = merged.copy()
    df["date"] = pd.to_datetime(df.get("date"), errors="coerce")
    df["month"] = df["date"].dt.to_period("M").dt.to_timestamp()

    paths = {}

    # Monthly revenue
    monthly = df.groupby("month", dropna=False)["revenue"].sum().reset_index().dropna(subset=["month"])
    if not monthly.empty:
        plt.figure()
        plt.plot(monthly["month"], monthly["revenue"])
        plt.title("Monthly Revenue")
        plt.xlabel("Month")
        plt.ylabel("Revenue")
        plt.xticks(rotation=45)
        plt.tight_layout()
        p = os.path.join(charts_dir, "monthly_revenue.png")
        plt.savefig(p, dpi=160)
        plt.close()
        paths["monthly_revenue"] = p

    # Top regions
    if "region" in df.columns:
        reg = df.groupby("region", dropna=False)["revenue"].sum().sort_values(ascending=False).head(10)
        if not reg.empty:
            plt.figure()
            reg.plot(kind="bar")
            plt.title("Top 10 Regions by Revenue")
            plt.xlabel("Region")
            plt.ylabel("Revenue")
            plt.tight_layout()
            p = os.path.join(charts_dir, "top_regions.png")
            plt.savefig(p, dpi=160)
            plt.close()
            paths["top_regions"] = p

    # Top products
    if "product" in df.columns:
        prod = df.groupby("product", dropna=False)["revenue"].sum().sort_values(ascending=False).head(10)
        if not prod.empty:
            plt.figure()
            prod.plot(kind="bar")
            plt.title("Top 10 Products by Revenue")
            plt.xlabel("Product")
            plt.ylabel("Revenue")
            plt.tight_layout()
            p = os.path.join(charts_dir, "top_products.png")
            plt.savefig(p, dpi=160)
            plt.close()
            paths["top_products"] = p

    # Revenue vs Royalty
    if "royalty_amount" in df.columns and df["revenue"].notna().any() and df["royalty_amount"].notna().any():
        plt.figure()
        plt.scatter(df["revenue"], df["royalty_amount"])
        plt.title("Revenue vs. Royalty Amount")
        plt.xlabel("Revenue")
        plt.ylabel("Royalty Amount")
        plt.tight_layout()
        p = os.path.join(charts_dir, "revenue_vs_royalty.png")
        plt.savefig(p, dpi=160)
        plt.close()
        paths["revenue_vs_royalty"] = p

    # Royalty rate histogram
    if "royalty_rate" in df.columns and df["royalty_rate"].notna().any():
        plt.figure()
        plt.hist(df["royalty_rate"].dropna(), bins=20)
        plt.title("Royalty Rate Distribution")
        plt.xlabel("Royalty Rate")
        plt.ylabel("Frequency")
        plt.tight_layout()
        p = os.path.join(charts_dir, "royalty_rate_hist.png")
        plt.savefig(p, dpi=160)
        plt.close()
        paths["royalty_rate_hist"] = p

    # Advanced: Projection chart
    grp = df.groupby("month")[["revenue","royalty_amount"]].sum().reset_index().sort_values("month")
    if not grp.empty:
        plt.figure()
        plt.plot(grp["month"], grp["revenue"], label="Revenue (historical)")
        x_future, y_future = simple_projection(grp["revenue"], 3)
        last_month = grp["month"].iloc[-1] if pd.notna(grp["month"].iloc[-1]) else datetime.today()
        future_months = pd.period_range(last_month.to_period("M")+1, periods=len(y_future), freq="M").to_timestamp()
        plt.plot(future_months, y_future, "r--", label="Revenue (forecast)")
        plt.legend()
        plt.title("Revenue Projection (next 3 months)")
        plt.xticks(rotation=45)
        plt.tight_layout()
        p = os.path.join(charts_dir, "revenue_projection.png")
        plt.savefig(p, dpi=160)
        plt.close()
        paths["revenue_projection"] = p

    # Advanced: Cumulative revenue by region (top 5)
    if "region" in df.columns:
        reg_month = df.groupby(["region","month"])["revenue"].sum().reset_index()
        top_regions = reg_month.groupby("region")["revenue"].sum().nlargest(5).index
        if len(top_regions) > 0:
            plt.figure()
            for r in top_regions:
                sub = reg_month[reg_month["region"]==r].set_index("month").sort_index()
                plt.plot(sub.index, sub["revenue"].cumsum(), label=r)
            plt.title("Cumulative Revenue by Top 5 Regions")
            plt.xlabel("Month")
            plt.ylabel("Cumulative Revenue")
            plt.legend()
            plt.xticks(rotation=45)
            plt.tight_layout()
            p = os.path.join(charts_dir, "cumulative_regions.png")
            plt.savefig(p, dpi=160)
            plt.close()
            paths["cumulative_regions"] = p

    # Advanced: Pareto products
    if "product" in df.columns:
        prod = df.groupby("product")["revenue"].sum().sort_values(ascending=False).head(20)
        if not prod.empty:
            cum_perc = (prod.cumsum()/prod.sum()*100).values
            fig, ax1 = plt.subplots()
            prod.plot(kind="bar", ax=ax1)
            ax1.set_ylabel("Revenue")
            ax2 = ax1.twinx()
            ax2.plot(cum_perc, marker="o", linestyle="--")
            ax2.set_ylabel("Cumulative %")
            ax1.set_title("Pareto Analysis: Top Products")
            fig.tight_layout()
            p = os.path.join(charts_dir, "pareto_products.png")
            plt.savefig(p, dpi=160)
            plt.close()
            paths["pareto_products"] = p

    return paths


def insert_charts_sheet(excel_writer, chart_paths: Dict[str, str]):
    # Creates a "Charts" sheet and inserts available PNGs
    wb = excel_writer.book
    sheet = wb.add_worksheet("Charts")
    row = 1
    col = 1
    for name, path in chart_paths.items():
        try:
            sheet.write(row, col, name.replace("_", " ").title())
            sheet.insert_image(row+1, col, path, {"x_scale": 0.8, "y_scale": 0.8})
            row += 25  # spacing between images
        except Exception:
            continue


def generate_report(input_paths: List[str], out_dir: str):
    os.makedirs(out_dir, exist_ok=True)
    canonical_frames = []
    role_maps = {}

    for p in input_paths:
        df = load_file(p)
        roles = infer_semantic_roles(df)
        role_maps[os.path.basename(p)] = roles
        unified = unify_to_canonical(df, roles, source_file=os.path.basename(p))
        canonical_frames.append(unified)

    merged = pd.concat(canonical_frames, ignore_index=True)

    insights = summarize_insights(merged)
    projections = build_projections(merged)

    # Save merged data
    merged_path = os.path.join(out_dir, "merged_canonical.csv")
    merged.to_csv(merged_path, index=False)

    # Save role mappings
    roles_path = os.path.join(out_dir, "role_mappings.json")
    with open(roles_path, "w", encoding="utf-8") as f:
        json.dump(role_maps, f, indent=2)

    # Save insights to Excel (+ charts sheet)
    excel_path = os.path.join(out_dir, "insights_report.xlsx")
    chart_paths = save_charts(merged, out_dir)
    with pd.ExcelWriter(excel_path, engine="xlsxwriter") as writer:
        merged.to_excel(writer, index=False, sheet_name="raw_merged")
        for name, df_ in insights.items():
            df_.to_excel(writer, index=False, sheet_name=name[:31])
        projections.to_excel(writer, index=False, sheet_name="projections")
        insert_charts_sheet(writer, chart_paths)

    # Markdown summary
    md_lines = []
    md_lines.append("# Smart Insights Summary\n")
    md_lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    md_lines.append("## Column Role Mapping by File\n")
    for fname, roles in role_maps.items():
        md_lines.append(f"**{fname}**")
        for k, v in roles.items():
            md_lines.append(f"- {k}: `{v}`")
        md_lines.append("")
    reg = insights["regional_trends"].groupby("region", dropna=False)["revenue"].sum().sort_values(ascending=False).head(10)
    md_lines.append("## Top Regions by Total Revenue")
    md_lines.append(reg.to_string())
    md_path = os.path.join(out_dir, "summary.md")
    with open(md_path, "w", encoding="utf-8") as f:
        f.write("\n".join(md_lines))

    return {
        "merged": merged_path,
        "roles": roles_path,
        "excel": excel_path,
        "summary": md_path,
        "charts_dir": os.path.join(out_dir, "charts")
    }


def main():
    parser = argparse.ArgumentParser(description="Analyze raw CSV/Excel files and generate insights + charts.")
    parser.add_argument("--in", dest="inputs", nargs="+", required=True, help="Input CSV/Excel files")
    parser.add_argument("--out", dest="out_dir", required=True, help="Output directory")
    args = parser.parse_args()

    outputs = generate_report(args.inputs, args.out_dir)
    print("Outputs written to:")
    for k, v in outputs.items():
        print(f"- {k}: {v}")


if __name__ == "__main__":
    main()
