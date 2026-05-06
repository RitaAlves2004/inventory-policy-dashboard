import os
import pandas as pd
import streamlit as st
import plotly.express as px

st.set_page_config(page_title="As Is vs Best Policy", layout="wide")

# ================= STYLE =================
st.markdown("""
<style>
.stApp{background:linear-gradient(180deg,#f8fafc 0%,#eef3f8 100%)}
section[data-testid="stSidebar"]{background:linear-gradient(180deg,#07113f 0%,#0b1f5c 100%)}
section[data-testid="stSidebar"] h1,section[data-testid="stSidebar"] h2,section[data-testid="stSidebar"] h3,
section[data-testid="stSidebar"] label,section[data-testid="stSidebar"] p,section[data-testid="stSidebar"] span{color:white!important}
section[data-testid="stSidebar"] div[data-baseweb="select"] span{color:#061243!important}
.block-container{padding-top:2rem;padding-bottom:4rem}
.dashboard-header,.global-kpi-box{background:white;border-radius:22px;border:1px solid rgba(6,18,67,.12);box-shadow:0 10px 28px rgba(6,18,67,.08)}
.dashboard-header{padding:28px 34px;margin-bottom:28px}
.dashboard-title{color:#061243;font-size:48px;font-weight:900;letter-spacing:-1px}
.dashboard-subtitle{color:#008080;font-size:18px;font-weight:700;margin-top:4px}
h1,h2,h3{color:#061243;font-weight:800!important}
.global-kpi-box{border:2px solid #061243;padding:32px 34px;margin:20px 0 35px;background:linear-gradient(180deg,#fff 0%,#f7fbfb 100%)}
.global-kpi-title{text-align:center;font-size:32px;font-weight:900;color:#061243;margin-bottom:28px}
table.global-kpi-table{width:100%;border-collapse:separate;border-spacing:0;overflow:hidden;border:1px solid #222;border-radius:14px;box-shadow:0 4px 12px rgba(0,0,0,.12);font-size:17px}
.global-kpi-table th{background:#101538;color:white;padding:16px;text-align:center;font-weight:800;border-right:1px solid #777}
.global-kpi-table td{padding:16px;text-align:center;border-right:1px solid #222;border-top:1px solid #222;color:#061243}
.global-kpi-table tr:nth-child(even) td{background:#edf7f7}
.global-kpi-table tr:nth-child(odd) td{background:white}
.global-kpi-table th:last-child,.global-kpi-table td:last-child{border-right:none}
.chart-container{
background: linear-gradient(180deg, #ffffff 0%, #f7fbfb 100%);
padding: 24px 26px 18px 26px;
border-radius: 26px;
border: 1px solid rgba(6,18,67,.10);
box-shadow: 0 14px 34px rgba(6,18,67,.10), 0 2px 8px rgba(6,18,67,.05);
margin-bottom: 28px;
}
.chart-frame-title{
color:#061243;
font-size:18px;
font-weight:900;
margin-bottom:4px;
}
</style>
""", unsafe_allow_html=True)

# ================= LOGOS =================
LOGO_1 = r"C:\Users\anatd\Downloads\FIM\Uni_Logo.png"
LOGO_2 = r"C:\Users\anatd\Downloads\FIM\LTP_Logo.png"

st.markdown('<div class="dashboard-header">', unsafe_allow_html=True)
c1, c2, c3 = st.columns([1, 1, 7])
c1.image(LOGO_1, width=105)
c2.image(LOGO_2, width=125)
c3.markdown("""
<div class="dashboard-title">As Is vs Best Policy</div>
<div class="dashboard-subtitle">Global Inventory Policy Comparison</div>
""", unsafe_allow_html=True)
st.markdown("</div>", unsafe_allow_html=True)

# ================= CONFIG =================
FOLDER = r"C:\Users\anatd\Downloads\FIM\parquet_filtered"

POLICIES = {
    "As Is": ("master_stock_forecast.parquet", "AsIsMetrics.csv"),
    "Smin-Smax Policy": ("PolíticaSminSmáx.csv", "PolíticaSminSmáx_KPIs.csv"),
    "Reorder Level Policy": ("PolíticaNívelDeEncomenda.csv", "PolíticaNívelDeEncomenda_KPIs.csv"),
    "Order Cycle Policy": ("PolíticaCicloDeEncomenda.csv", "PolíticaCicloDeEncomenda_KPIs.csv"),
}

KPI_ORDER = [
    "Total Cost",
    "Stock Out Rate (%)",
    "Alpha Service Level (%)",
    "Beta Service Level (%)",
    "Average Inventory Level",
    "Stock Coverage (days)",
]

# ================= FUNCTIONS =================
@st.cache_data
def load_csv(path):
    return pd.read_csv(path, sep=";", decimal=",", encoding="utf-8-sig")


@st.cache_data
def load_data(path):
    if path.endswith(".parquet"):
        return pd.read_parquet(path)
    return pd.read_csv(path, sep=";", decimal=",", encoding="utf-8-sig")


def normalize_kpis(df):
    rename_map = {
        "SKU": "sku",
        "ABC_Class": "ABC Class",
        "XYZ_Class": "XYZ Class",
        "XYZ Class": "XYZ Class",

        "Total Cost": "Total Cost",
        "total_cost": "Total Cost",
        "Total_Cost": "Total Cost",

        "Stockout Rate": "Stock Out Rate (%)",
        "stock_out_rate_pct": "Stock Out Rate (%)",
        "stock_out_rate_%": "Stock Out Rate (%)",
        "stock_out_rate": "Stock Out Rate (%)",

        "Alpha Service Level": "Alpha Service Level (%)",
        "alpha_service_level": "Alpha Service Level (%)",
        "alpha_service_level_%": "Alpha Service Level (%)",

        "Beta Service Level": "Beta Service Level (%)",
        "beta_service_level": "Beta Service Level (%)",
        "beta_service_level_%": "Beta Service Level (%)",

        "average_inventory_level": "Average Inventory Level",
        "average_inventory_level_quantidade": "Average Inventory Level",

        "stock_coverage_days": "Stock Coverage (days)",
        "stock_coverage_dias": "Stock Coverage (days)",
    }

    df = df.rename(columns=rename_map)
    df = df.loc[:, ~df.columns.duplicated()]

    cols = ["sku", "ABC Class", "XYZ Class"] + KPI_ORDER
    return df[[c for c in cols if c in df.columns]]


def build_global_kpi_comparison():
    rows = []
    common_skus = None

    for policy, (sim_file, _) in POLICIES.items():
        path = os.path.join(FOLDER, sim_file)

        if not os.path.exists(path):
            continue

        if policy == "As Is":
            temp = pd.read_parquet(path)["sku"].dropna().astype(str).unique()
        else:
            temp = load_data(path)["SKU"].dropna().astype(str).unique()

        temp = set(temp)
        common_skus = temp if common_skus is None else common_skus & temp

    if common_skus is None:
        common_skus = set()

    for policy, (simulation_file, kpis_file) in POLICIES.items():
        kpis_path = os.path.join(FOLDER, kpis_file)
        simulation_path = os.path.join(FOLDER, simulation_file)

        if not os.path.exists(kpis_path) or not os.path.exists(simulation_path):
            continue

        df = normalize_kpis(load_csv(kpis_path))

        if "sku" not in df.columns:
            continue

        df["sku"] = df["sku"].astype(str)
        df = df[df["sku"].isin(common_skus)]

        if df.empty:
            continue

        if policy == "As Is":
            sim_df = pd.read_parquet(simulation_path).rename(columns={
                "stock_on_hand": "SOH End",
                "demand": "Demand",
                "date": "Date",
                "sku": "sku"
            })

            sim_df["Date"] = pd.to_datetime(
                sim_df["Date"].astype(str),
                format="%Y%m%d",
                errors="coerce"
            )

            sim_df = sim_df[sim_df["Date"] >= pd.Timestamp("2023-06-01")]

        else:
            sim_df = load_data(simulation_path).rename(columns={
                "SKU": "sku",
                "Demand": "Demand",
                "SOH End": "SOH End",
                "Date": "Date"
            })

            sim_df["Date"] = pd.to_datetime(
                sim_df["Date"],
                dayfirst=True,
                errors="coerce"
            )

        sim_df["sku"] = sim_df["sku"].astype(str)
        sim_df = sim_df[sim_df["sku"].isin(common_skus)]

        total_soh = sim_df["SOH End"].sum()
        total_demand = sim_df["Demand"].sum()

        global_stock_coverage = total_soh / total_demand if total_demand > 0 else 0

        values = {
            "Total Cost": df["Total Cost"].sum(),
            "Stock Out Rate (%)": df["Stock Out Rate (%)"].mean(),
            "Alpha Service Level (%)": df["Alpha Service Level (%)"].mean(),
            "Beta Service Level (%)": df["Beta Service Level (%)"].mean(),
            "Average Inventory Level": df["Average Inventory Level"].mean(),
            "Stock Coverage (days)": global_stock_coverage,
        }

        rows += [
            {"KPI": k, "Policy": policy, "Value": round(v, 2)}
            for k, v in values.items()
        ]

    if not rows:
        return pd.DataFrame()

    pivot = pd.DataFrame(rows).pivot(
        index="KPI",
        columns="Policy",
        values="Value"
    ).reset_index()

    pivot["KPI"] = pd.Categorical(
        pivot["KPI"],
        categories=KPI_ORDER,
        ordered=True
    )

    return pivot.sort_values("KPI")


def render_global_kpi_table(df, title="Global KPIs by Policy"):
    if df.empty:
        st.warning("No global KPI data available.")
        return

    header = "".join(f"<th>{c}</th>" for c in df.columns)
    body = ""

    for _, row in df.iterrows():
        cells = "".join(
            f"<td>{row[c] if c == 'KPI' else f'{float(row[c]):.2f}'}</td>"
            for c in df.columns
        )
        body += f"<tr>{cells}</tr>"

    st.markdown(f"""
    <div class="global-kpi-box">
        <div class="global-kpi-title">{title}</div>
        <table class="global-kpi-table">
            <thead><tr>{header}</tr></thead>
            <tbody>{body}</tbody>
        </table>
    </div>
    """, unsafe_allow_html=True)


def score_lower(s):
    return pd.Series([1] * len(s), index=s.index) if s.max() == s.min() else (s.max() - s) / (s.max() - s.min())


def score_higher(s):
    return pd.Series([1] * len(s), index=s.index) if s.max() == s.min() else (s - s.min()) / (s.max() - s.min())


def get_best_policy(global_kpis_df):
    if global_kpis_df.empty:
        return None

    df = global_kpis_df.copy().set_index("KPI")
    policies = [c for c in df.columns if c != "As Is"]

    if not policies:
        return None

    score_df = pd.DataFrame(index=policies)

    score_df["Total Cost"] = [df.loc["Total Cost", p] for p in policies]
    score_df["Beta Service Level (%)"] = [df.loc["Beta Service Level (%)", p] for p in policies]
    score_df["Average Inventory Level"] = [df.loc["Average Inventory Level", p] for p in policies]

    score_df["Cost Score"] = score_lower(score_df["Total Cost"])
    score_df["Inventory Score"] = score_lower(score_df["Average Inventory Level"])
    score_df["Service Score"] = score_higher(score_df["Beta Service Level (%)"])

    score_df["Trade-off Score"] = score_df[
        ["Cost Score", "Inventory Score", "Service Score"]
    ].mean(axis=1)

    return score_df["Trade-off Score"].idxmax(), score_df.round(4)


def load_policy_simulation(policy_name):
    simulation_file, _ = POLICIES[policy_name]
    simulation_path = os.path.join(FOLDER, simulation_file)

    if policy_name == "As Is":
        sim_df = pd.read_parquet(simulation_path).rename(columns={
            "stock_on_hand": "soh_final"
        })

        sim_df["date"] = pd.to_datetime(
            sim_df["date"].astype(str),
            format="%Y%m%d",
            errors="coerce"
        )

        sim_df = sim_df[sim_df["date"] >= pd.Timestamp("2023-06-01")]

    else:
        sim_df = load_data(simulation_path).rename(columns={
            "SKU": "sku",
            "Date": "date",
            "Demand": "demand",
            "SOH End": "soh_final",
        })

        sim_df["date"] = pd.to_datetime(
            sim_df["date"],
            dayfirst=True,
            errors="coerce"
        )

    sim_df = sim_df.dropna(subset=["date"])
    sim_df["sku"] = sim_df["sku"].astype(str)

    return sim_df


def get_common_skus():
    common_skus = None

    for policy, (sim_file, _) in POLICIES.items():
        path = os.path.join(FOLDER, sim_file)

        if not os.path.exists(path):
            continue

        if policy == "As Is":
            temp = pd.read_parquet(path)["sku"].dropna().astype(str).unique()
        else:
            temp = load_data(path)["SKU"].dropna().astype(str).unique()

        temp = set(temp)
        common_skus = temp if common_skus is None else common_skus & temp

    return common_skus if common_skus is not None else set()


def get_segment_skus(abc_class, xyz_class):
    asis_kpis_path = os.path.join(FOLDER, POLICIES["As Is"][1])

    if not os.path.exists(asis_kpis_path):
        return set()

    asis_kpis = normalize_kpis(load_csv(asis_kpis_path))

    required_cols = {"sku", "ABC Class", "XYZ Class"}

    if not required_cols.issubset(asis_kpis.columns):
        return set()

    segment_df = asis_kpis[
        (asis_kpis["ABC Class"].astype(str).str.upper().str.strip() == abc_class) &
        (asis_kpis["XYZ Class"].astype(str).str.upper().str.strip() == xyz_class)
    ]

    return set(segment_df["sku"].dropna().astype(str))


def build_kpi_comparison_for_skus(selected_skus):
    rows = []
    selected_skus = set(selected_skus)

    if not selected_skus:
        return pd.DataFrame()

    for policy, (simulation_file, kpis_file) in POLICIES.items():
        kpis_path = os.path.join(FOLDER, kpis_file)
        simulation_path = os.path.join(FOLDER, simulation_file)

        if not os.path.exists(kpis_path) or not os.path.exists(simulation_path):
            continue

        df = normalize_kpis(load_csv(kpis_path))

        if "sku" not in df.columns:
            continue

        df["sku"] = df["sku"].astype(str)
        df = df[df["sku"].isin(selected_skus)]

        if df.empty:
            continue

        if policy == "As Is":
            sim_df = pd.read_parquet(simulation_path).rename(columns={
                "stock_on_hand": "SOH End",
                "demand": "Demand",
                "date": "Date",
                "sku": "sku"
            })

            sim_df["Date"] = pd.to_datetime(
                sim_df["Date"].astype(str),
                format="%Y%m%d",
                errors="coerce"
            )

            sim_df = sim_df[sim_df["Date"] >= pd.Timestamp("2023-06-01")]

        else:
            sim_df = load_data(simulation_path).rename(columns={
                "SKU": "sku",
                "Demand": "Demand",
                "SOH End": "SOH End",
                "Date": "Date"
            })

            sim_df["Date"] = pd.to_datetime(
                sim_df["Date"],
                dayfirst=True,
                errors="coerce"
            )

        sim_df["sku"] = sim_df["sku"].astype(str)
        sim_df = sim_df[sim_df["sku"].isin(selected_skus)]

        total_soh = sim_df["SOH End"].sum()
        total_demand = sim_df["Demand"].sum()

        global_stock_coverage = total_soh / total_demand if total_demand > 0 else 0

        values = {
            "Total Cost": df["Total Cost"].sum(),
            "Stock Out Rate (%)": df["Stock Out Rate (%)"].mean(),
            "Alpha Service Level (%)": df["Alpha Service Level (%)"].mean(),
            "Beta Service Level (%)": df["Beta Service Level (%)"].mean(),
            "Average Inventory Level": df["Average Inventory Level"].mean(),
            "Stock Coverage (days)": global_stock_coverage,
        }

        rows += [
            {"KPI": k, "Policy": policy, "Value": round(v, 2)}
            for k, v in values.items()
        ]

    if not rows:
        return pd.DataFrame()

    pivot = pd.DataFrame(rows).pivot(
        index="KPI",
        columns="Policy",
        values="Value"
    ).reset_index()

    pivot["KPI"] = pd.Categorical(
        pivot["KPI"],
        categories=KPI_ORDER,
        ordered=True
    )

    return pivot.sort_values("KPI")

def get_best_policy_by_segment(global_kpis_df, abc_class):
    if global_kpis_df.empty:
        return None

    df = global_kpis_df.copy().set_index("KPI")
    policies = [c for c in df.columns if c != "As Is"]

    if not policies:
        return None

    score_df = pd.DataFrame(index=policies)

    score_df["Total Cost"] = [df.loc["Total Cost", p] for p in policies]
    score_df["Beta Service Level (%)"] = [df.loc["Beta Service Level (%)", p] for p in policies]
    score_df["Average Inventory Level"] = [df.loc["Average Inventory Level", p] for p in policies]

    score_df["Cost Score"] = score_lower(score_df["Total Cost"])
    score_df["Inventory Score"] = score_lower(score_df["Average Inventory Level"])
    score_df["Service Score"] = score_higher(score_df["Beta Service Level (%)"])

    if abc_class == "A":
        service_weight = 0.50
        cost_weight = 0.30
        inventory_weight = 0.20
    elif abc_class == "B":
        service_weight = 0.40
        cost_weight = 0.35
        inventory_weight = 0.25
    else:
        service_weight = 0.30
        cost_weight = 0.40
        inventory_weight = 0.30

    score_df["Trade-off Score"] = (
        cost_weight * score_df["Cost Score"] +
        inventory_weight * score_df["Inventory Score"] +
        service_weight * score_df["Service Score"]
    )

    return score_df["Trade-off Score"].idxmax(), score_df.round(4)

def build_best_policy_by_abc_xyz():
    rows = []
    common_skus = get_common_skus()

    for abc_class in ["A", "B", "C"]:
        for xyz_class in ["X", "Y", "Z"]:
            segment_skus = get_segment_skus(abc_class, xyz_class)
            segment_skus = segment_skus & common_skus

            if not segment_skus:
                continue

            segment_kpis = build_kpi_comparison_for_skus(segment_skus)

            if segment_kpis.empty:
                continue

            best_policy_result = get_best_policy_by_segment(segment_kpis, abc_class)

            if best_policy_result is None:
                continue

            segment_best_policy, segment_score_df = best_policy_result

            rows.append({
                "ABC Class": abc_class,
                "XYZ Class": xyz_class,
                "ABC-XYZ Segment": f"{abc_class}-{xyz_class}",
                "Best Policy": segment_best_policy,
                "SKUs": len(segment_skus),
                "Trade-off Score": segment_score_df.loc[segment_best_policy, "Trade-off Score"]
            })

    return pd.DataFrame(rows)


# ================= PAGE =================
global_kpis_df = build_global_kpi_comparison()

if global_kpis_df.empty:
    st.error("No KPI data available for comparison.")
    st.stop()

best_policy_result = get_best_policy(global_kpis_df)

if best_policy_result is None:
    st.error("Could not identify the best policy.")
    st.stop()

best_policy, score_df = best_policy_result

st.info(f"Best Policy selected: **{best_policy}**")

# ================= KPI TABLE =================
comparison_kpis = global_kpis_df[["KPI", "As Is", best_policy]]
render_global_kpi_table(
    comparison_kpis,
    title=f"Global KPI Comparison: As Is vs {best_policy}"
)

# ================= BEST POLICY BY ABC-XYZ TABLE =================
segment_best_df = build_best_policy_by_abc_xyz()

st.markdown("### Best Policy by ABC-XYZ Category")

if segment_best_df.empty:
    st.warning("No ABC-XYZ segment data available.")
else:
    st.dataframe(
    segment_best_df[
        ["ABC Class", "XYZ Class", "ABC-XYZ Segment", "Best Policy", "SKUs", "Trade-off Score"]
    ].reset_index(drop=True),
    use_container_width=True,
    hide_index=True
)

# ================= CHART FILTER =================
chart_scope = "Global"

if not segment_best_df.empty:
    segment_options = ["Global"] + segment_best_df["ABC-XYZ Segment"].tolist()

    chart_scope = st.selectbox(
        "Select ABC-XYZ category for chart",
        segment_options
    )

# ================= LOAD SIMULATION DATA =================
if chart_scope == "Global":
    chart_best_policy = best_policy
    chart_skus = None
    chart_title_suffix = f"As Is vs {chart_best_policy}"
else:
    selected_segment = segment_best_df[
        segment_best_df["ABC-XYZ Segment"] == chart_scope
    ].iloc[0]

    selected_abc = selected_segment["ABC Class"]
    selected_xyz = selected_segment["XYZ Class"]
    chart_best_policy = selected_segment["Best Policy"]

    chart_skus = get_segment_skus(selected_abc, selected_xyz)
    chart_skus = chart_skus & get_common_skus()

    chart_title_suffix = f"{chart_scope} | As Is vs {chart_best_policy}"

asis_sim = load_policy_simulation("As Is")
best_sim = load_policy_simulation(chart_best_policy)

# Keep only SKUs common to both policies
common_skus = set(asis_sim["sku"].dropna().astype(str)) & set(best_sim["sku"].dropna().astype(str))

if chart_skus is not None:
    common_skus = common_skus & chart_skus

asis_sim = asis_sim[asis_sim["sku"].isin(common_skus)]
best_sim = best_sim[best_sim["sku"].isin(common_skus)]

# Aggregate all SKUs by date
asis_total_df = (
    asis_sim
    .groupby("date", as_index=False)
    .agg(
        demand=("demand", "sum"),
        soh_final=("soh_final", "sum")
    )
)

best_total_df = (
    best_sim
    .groupby("date", as_index=False)
    .agg(
        demand=("demand", "sum"),
        soh_final=("soh_final", "sum")
    )
)

asis_total_df["Policy"] = "As Is"
best_total_df["Policy"] = chart_best_policy

comparison_df = pd.concat(
    [asis_total_df, best_total_df],
    ignore_index=True
)

# ================= CHART =================
demand_df = (
    asis_total_df[["date", "demand"]]
    .rename(columns={"demand": "Value"})
)

demand_df["Metric"] = "Total Demand"

asis_stock_df = (
    asis_total_df[["date", "soh_final"]]
    .rename(columns={"soh_final": "Value"})
)

asis_stock_df["Metric"] = "Stock On Hand - As Is"

best_stock_df = (
    best_total_df[["date", "soh_final"]]
    .rename(columns={"soh_final": "Value"})
)

best_stock_df["Metric"] = f"Stock On Hand - {chart_best_policy}"

chart_long = pd.concat(
    [demand_df, asis_stock_df, best_stock_df],
    ignore_index=True
)

fig = px.line(
    chart_long,
    x="date",
    y="Value",
    color="Metric",
    markers=False,
    title=f"Total Demand vs Total Stock On Hand Over Time | {chart_title_suffix}",
    color_discrete_sequence=[
        "#008080",
        "#061243",
        "#00b3b3"
    ]
)

fig.update_layout(
    xaxis_title="Date",
    yaxis_title="Total Demand / Total Stock On Hand",
    legend_title="Metric",
    hovermode="x unified",
    paper_bgcolor="white",
    plot_bgcolor="white",
    title_font=dict(size=22, color="#061243"),
    font=dict(color="#061243"),
    height=650
)

st.plotly_chart(fig, use_container_width=True)

# ================= SCORE TABLE =================
with st.expander("Show best policy score calculation"):
    st.dataframe(score_df, use_container_width=True)