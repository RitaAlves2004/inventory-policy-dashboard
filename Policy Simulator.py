import os
import pandas as pd
import streamlit as st
import plotly.express as px

st.set_page_config(page_title="Policy Simulator", layout="wide")

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
div[data-testid="stMetric"]{background:white;border:1px solid rgba(6,18,67,.12);border-left:6px solid #008080;padding:18px 20px;border-radius:18px;box-shadow:0 8px 22px rgba(6,18,67,.08)}
div[data-testid="stMetricLabel"]{color:#061243!important;font-weight:700}
div[data-testid="stMetricValue"]{color:#061243!important;font-size:32px!important;font-weight:850!important}
.stDataFrame,div[data-testid="stExpander"]{background:white;border-radius:16px;box-shadow:0 8px 20px rgba(6,18,67,.08)}
div[data-testid="stExpander"]{border:1px solid rgba(6,18,67,.12)}
.global-kpi-box{border:2px solid #061243;padding:32px 34px;margin:20px 0 35px;background:linear-gradient(180deg,#fff 0%,#f7fbfb 100%)}
.global-kpi-title{text-align:center;font-size:32px;font-weight:900;color:#061243;margin-bottom:28px}
table.global-kpi-table{width:100%;border-collapse:separate;border-spacing:0;overflow:hidden;border:1px solid #222;border-radius:14px;box-shadow:0 4px 12px rgba(0,0,0,.12);font-size:17px}
.global-kpi-table th{background:#101538;color:white;padding:16px;text-align:center;font-weight:800;border-right:1px solid #777}
.global-kpi-table td{padding:16px;text-align:center;border-right:1px solid #222;border-top:1px solid #222;color:#061243}
.global-kpi-table tr:nth-child(even) td{background:#edf7f7}
.global-kpi-table tr:nth-child(odd) td{background:white}
.global-kpi-table th:last-child,.global-kpi-table td:last-child{border-right:none}
.chart-container{background:linear-gradient(180deg,#ffffff 0%,#f7fbfb 100%);padding:24px 26px 18px 26px;border-radius:26px;border:1px solid rgba(6,18,67,.10);box-shadow:0 14px 34px rgba(6,18,67,.10),0 2px 8px rgba(6,18,67,.05);margin-bottom:28px}
.chart-container:hover{box-shadow:0 18px 42px rgba(6,18,67,.15),0 4px 12px rgba(6,18,67,.08)}
.chart-frame-title{color:#061243;font-size:18px;font-weight:900;margin-bottom:4px}
div[data-testid="stExpander"]{background:linear-gradient(180deg,#ffffff 0%,#f8fcfc 100%)!important;border-radius:20px!important;border:1px solid rgba(6,18,67,.10)!important;overflow:hidden!important;box-shadow:0 10px 26px rgba(6,18,67,.08)!important;margin-bottom:16px!important}
.stDataFrame{border-radius:22px!important;overflow:hidden!important;border:1px solid rgba(6,18,67,.10)!important;box-shadow:0 12px 28px rgba(6,18,67,.08)!important;background:white!important}
div.stButton > button{width:100%;height:58px;border-radius:16px;border:none;background:linear-gradient(135deg,#061243 0%,#008080 100%);color:white;font-size:18px;font-weight:800;box-shadow:0 8px 20px rgba(6,18,67,.18);transition:all .25s ease}
div.stButton > button:hover{transform:translateY(-3px);box-shadow:0 12px 24px rgba(6,18,67,.28);background:linear-gradient(135deg,#008080 0%,#061243 100%);color:white}
div.stButton > button:focus{border:2px solid #00b3b3!important;color:white!important}
div.stButton > button[kind="primary"]{background:linear-gradient(135deg,#00a6a6 0%,#061243 100%);border:2px solid #00d4d4}
</style>
""", unsafe_allow_html=True)

# ================= LOGOS =================

LOGO_1 = "Uni_Logo.png"
LOGO_2 = "LTP_Logo.png"

st.markdown('<div class="dashboard-header">', unsafe_allow_html=True)
c1, c2, c3 = st.columns([1, 1, 7])
c1.image(LOGO_1, width=105)
c2.image(LOGO_2, width=125)
c3.markdown("""
<div class="dashboard-title">Policy Simulator</div>
<div class="dashboard-subtitle">Inventory Policy Analysis Dashboard</div>
""", unsafe_allow_html=True)
st.markdown("</div>", unsafe_allow_html=True)

# ================= CONFIG =================

FOLDER = "."

POLICIES = {
    "As Is": ("master_stock_forecast.parquet", "AsIsMetrics.csv"),
    "Smin-Smax Policy": ("PolíticaSminSmáx.parquet", "PolíticaSminSmáx_KPIs.csv"),
    "Reorder Level Policy": ("PolíticaNívelDeEncomenda.parquet", "PolíticaNívelDeEncomenda_KPIs.csv"),
    "Order Cycle Policy": ("PolíticaCicloDeEncomenda.parquet", "PolíticaCicloDeEncomenda_KPIs.csv"),
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
    df = df.copy()
    df.columns = df.columns.str.strip()

    rename_map = {
        "SKU": "sku",
        "sku": "sku",

        "ABC_Class": "ABC Class",
        "ABC Class": "ABC Class",

        "Total Cost": "Total Cost",
        "total_cost": "Total Cost",

        "Stock Cost": "Stock Cost",
        "stock_cost": "Stock Cost",
        "total_holding_cost": "Stock Cost",
        "custo_stock_total": "Stock Cost",

        "Stockout Rate": "Stock Out Rate (%)",
        "Stock Out Rate": "Stock Out Rate (%)",
        "Stock Out Rate (%)": "Stock Out Rate (%)",
        "stock_out_rate": "Stock Out Rate (%)",
        "stock_out_rate_pct": "Stock Out Rate (%)",
        "stock_out_rate_%": "Stock Out Rate (%)",

        "Alpha Service Level": "Alpha Service Level (%)",
        "Alpha Service Level (%)": "Alpha Service Level (%)",
        "alpha_service_level": "Alpha Service Level (%)",
        "alpha_service_level_%": "Alpha Service Level (%)",

        "Beta Service Level": "Beta Service Level (%)",
        "Beta Service Level (%)": "Beta Service Level (%)",
        "beta_service_level": "Beta Service Level (%)",
        "beta_service_level_%": "Beta Service Level (%)",

        "Average Inventory Level": "Average Inventory Level",
        "average_inventory_level": "Average Inventory Level",
        "average_inventory_level_quantidade": "Average Inventory Level",

        "Stock Coverage (days)": "Stock Coverage (days)",
        "stock_coverage_days": "Stock Coverage (days)",
        "stock_coverage_dias": "Stock Coverage (days)",
    }

    df = df.rename(columns=rename_map)

    if "Total Cost" not in df.columns and "Stock Cost" in df.columns:
        df["Total Cost"] = df["Stock Cost"]

    required_cols = ["sku", "ABC Class"] + KPI_ORDER

    for col in required_cols:
        if col not in df.columns:
            df[col] = pd.NA

    return df[required_cols]


def build_global_kpi_comparison(abc_filter="Total SKUs"):
    rows = []
    common_skus = None
    allowed_skus = None

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
        return pd.DataFrame(columns=["KPI"])

    if abc_filter != "Total SKUs":
        asis_path = os.path.join(FOLDER, POLICIES["As Is"][1])
        asis_df = normalize_kpis(load_csv(asis_path))

        allowed_skus = set(
            asis_df.loc[
                asis_df["ABC Class"].astype(str).str.upper() == abc_filter,
                "sku"
            ].dropna().astype(str)
        ) & common_skus

    for policy, (simulation_file, kpis_file) in POLICIES.items():
        kpis_path = os.path.join(FOLDER, kpis_file)
        simulation_path = os.path.join(FOLDER, simulation_file)

        if not os.path.exists(kpis_path) or not os.path.exists(simulation_path):
            continue

        df = normalize_kpis(load_csv(kpis_path))
        df["sku"] = df["sku"].astype(str)

        selected_skus = common_skus if abc_filter == "Total SKUs" else allowed_skus
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

        total_soh = pd.to_numeric(sim_df["SOH End"], errors="coerce").sum()
        total_demand = pd.to_numeric(sim_df["Demand"], errors="coerce").sum()

        global_stock_coverage = total_soh / total_demand if total_demand > 0 else 0

        values = {
            "Total Cost": pd.to_numeric(df["Total Cost"], errors="coerce").sum(),
            "Stock Out Rate (%)": pd.to_numeric(df["Stock Out Rate (%)"], errors="coerce").mean(),
            "Alpha Service Level (%)": pd.to_numeric(df["Alpha Service Level (%)"], errors="coerce").mean(),
            "Beta Service Level (%)": pd.to_numeric(df["Beta Service Level (%)"], errors="coerce").mean(),
            "Average Inventory Level": pd.to_numeric(df["Average Inventory Level"], errors="coerce").mean(),
            "Stock Coverage (days)": global_stock_coverage,
        }

        rows += [
            {"KPI": k, "Policy": policy, "Value": round(v, 2)}
            for k, v in values.items()
        ]

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


def render_global_kpi_table(df):
    header = "".join(f"<th>{c}</th>" for c in df.columns)

    body = ""
    for _, row in df.iterrows():
        cells = ""
        for c in df.columns:
            if c == "KPI":
                value = row[c]
            else:
                value = "" if pd.isna(row[c]) else f"{float(row[c]):.2f}"
            cells += f"<td>{value}</td>"
        body += f"<tr>{cells}</tr>"

    st.markdown(f"""
    <div class="global-kpi-box">
        <div class="global-kpi-title">Global KPIs by Policy</div>
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


def render_monte_carlo_analysis():
    mc_path = os.path.join(FOLDER, "MonteCarlo_LeadTime_AllPolicies.csv")

    if not os.path.exists(mc_path):
        st.warning(f"Monte Carlo file not found: {mc_path}")
        return

    mc_df = load_csv(mc_path)

    if "Stock Cost" in mc_df.columns and "Total Cost" not in mc_df.columns:
        mc_df["Total Cost"] = mc_df["Stock Cost"]

    required_cols = [
        "Policy",
        "Simulation",
        "Total Cost",
        "Beta Service Level (%)",
        "Average Inventory Level"
    ]

    missing_cols = [c for c in required_cols if c not in mc_df.columns]

    if missing_cols:
        st.error("Missing Monte Carlo columns: " + ", ".join(missing_cols))
        return

    for col in [
        "Total Cost",
        "Stock Out Rate (%)",
        "Alpha Service Level (%)",
        "Beta Service Level (%)",
        "Average Inventory Level",
        "Stock Coverage (days)"
    ]:
        if col in mc_df.columns:
            mc_df[col] = pd.to_numeric(mc_df[col], errors="coerce")

    mc_df = mc_df.dropna(subset=[
        "Policy",
        "Total Cost",
        "Beta Service Level (%)",
        "Average Inventory Level"
    ])

    if mc_df.empty:
        st.warning("Monte Carlo file has no valid data.")
        return

    summary_df = (
        mc_df.groupby("Policy", as_index=False)
        .agg(
            total_cost=("Total Cost", "mean"),
            beta_service_level=("Beta Service Level (%)", "mean"),
            average_inventory_level=("Average Inventory Level", "mean"),
            stock_coverage=("Stock Coverage (days)", "mean")
        )
    )

    summary_df["Inventory Score"] = score_lower(summary_df["average_inventory_level"])
    summary_df["Cost Score"] = score_lower(summary_df["total_cost"])
    summary_df["Service Score"] = score_higher(summary_df["beta_service_level"])
    summary_df["Trade-off Score"] = summary_df[
        ["Inventory Score", "Cost Score", "Service Score"]
    ].mean(axis=1)

    best_policy = summary_df.loc[summary_df["Trade-off Score"].idxmax()]

    st.markdown("---")
    st.markdown("""
    <h2 style="text-align:center;color:#061243;font-weight:900;margin-top:30px;margin-bottom:30px;">
        Monte Carlo Simulation
    </h2>
    """, unsafe_allow_html=True)

    col_left, col_right = st.columns([1.35, 1])

    with col_left:
        fig = px.scatter(
            mc_df,
            x="Average Inventory Level",
            y="Beta Service Level (%)",
            color="Policy",
            symbol="Policy",
            size="Total Cost",
            size_max=10,
            title="Monte Carlo Scenarios: Inventory vs Service Level",
            hover_data={
                "Policy": True,
                "Simulation": True,
                "Total Cost": ":,.2f",
                "Average Inventory Level": ":,.2f",
                "Beta Service Level (%)": ":,.2f",
                "Stock Coverage (days)": ":,.2f",
            }
        )

        fig.update_traces(
            opacity=0.65,
            marker=dict(line=dict(width=0.5, color="white"))
        )

        fig.update_layout(
            xaxis_title="Average Inventory Level",
            yaxis_title="β Service Level (%)",
            height=650,
            margin=dict(l=70, r=100, t=90, b=100),
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=-0.25,
                xanchor="center",
                x=0.5
            ),
            paper_bgcolor="white",
            plot_bgcolor="white",
            title_font=dict(size=22, color="#061243"),
            font=dict(color="#061243")
        )

        st.plotly_chart(fig, use_container_width=True)

    with col_right:
        st.markdown("### Monte Carlo Insights")

        st.info(
            f"**Best Trade-off:** {best_policy['Policy']}  \n\n"
            f"Best average balance between inventory level, total cost and β service level."
        )

        cards = [
            ("Lowest Total Cost", summary_df.loc[summary_df["total_cost"].idxmin()], "total_cost", "Total Cost", "#061243"),
            ("Highest Service Level", summary_df.loc[summary_df["beta_service_level"].idxmax()], "beta_service_level", "Beta Service Level (%)", "#008080"),
            ("Lowest Inventory Level", summary_df.loc[summary_df["average_inventory_level"].idxmin()], "average_inventory_level", "Average Inventory Level", "#ff7f32"),
        ]

        html = ""
        for title, row, metric_col, metric_label, color in cards:
            suffix = "%" if metric_label == "Beta Service Level (%)" else ""

            html += f"""
            <div style="background:white;padding:22px;border-radius:14px;border-left:6px solid {color};
            margin-bottom:18px;box-shadow:0 8px 20px rgba(6,18,67,.08);">
                <h4 style="color:{color};">{title}</h4>
                <p><b>{row['Policy']}</b></p>
                <p>{metric_label}: {row[metric_col]:,.2f}{suffix}</p>
            </div>
            """

        st.markdown(html, unsafe_allow_html=True)

    with st.expander("Show Monte Carlo simulation results"):
        st.dataframe(mc_df, use_container_width=True)


# ================= SIDEBAR =================

st.sidebar.title("Filters")

policy_name = st.sidebar.selectbox("Select Policy", list(POLICIES.keys()))
simulation_file, kpis_file = POLICIES[policy_name]

simulation_path = os.path.join(FOLDER, simulation_file)
kpis_path = os.path.join(FOLDER, kpis_file)

for path, label in [(simulation_path, "Simulation"), (kpis_path, "KPI")]:
    if not os.path.exists(path):
        st.error(f"{label} file not found: {path}")
        st.stop()

sku_kpis_all = normalize_kpis(load_csv(kpis_path))

if policy_name == "As Is":
    df = pd.read_parquet(simulation_path).rename(columns={"stock_on_hand": "soh_final"})
    df["date"] = pd.to_datetime(df["date"].astype(str), format="%Y%m%d", errors="coerce")
    df = df[df["date"] >= pd.Timestamp("2023-06-01")]
else:
    df = load_data(simulation_path).rename(columns={
        "SKU": "sku",
        "Date": "date",
        "Demand": "demand",
        "SOH End": "soh_final",
    })
    df["date"] = pd.to_datetime(df["date"], dayfirst=True, errors="coerce")

df = df.dropna(subset=["date"])
df["sku"] = df["sku"].astype(str)
sku_kpis_all["sku"] = sku_kpis_all["sku"].astype(str)

# ================= SKU FILTER =================

if policy_name == "As Is":
    optimized_sku_sets = []

    for optimized_policy in [
        "Smin-Smax Policy",
        "Reorder Level Policy",
        "Order Cycle Policy"
    ]:
        _, optimized_kpis_file = POLICIES[optimized_policy]
        optimized_kpis_path = os.path.join(FOLDER, optimized_kpis_file)

        if os.path.exists(optimized_kpis_path):
            optimized_kpis_df = normalize_kpis(load_csv(optimized_kpis_path))

            if "sku" in optimized_kpis_df.columns:
                optimized_sku_sets.append(
                    set(optimized_kpis_df["sku"].dropna().astype(str))
                )

    if optimized_sku_sets:
        valid_asis_skus = set.intersection(*optimized_sku_sets)
        available_skus = sorted(set(df["sku"].dropna()).intersection(valid_asis_skus))
    else:
        available_skus = sorted(df["sku"].dropna().unique())

else:
    available_skus = sorted(df["sku"].dropna().unique())

if not available_skus:
    st.error("No valid SKUs available for the selected policy.")
    st.stop()

selected_sku = st.sidebar.selectbox("Select SKU", available_skus)

sku_kpis = sku_kpis_all[sku_kpis_all["sku"] == str(selected_sku)]
sku_df = df[df["sku"] == str(selected_sku)].sort_values("date")

# ================= ABC FILTER =================

if "abc_filter" not in st.session_state:
    st.session_state.abc_filter = "Total SKUs"

st.markdown("""
<h3 style="color:#061243;font-weight:900;margin-bottom:18px;margin-top:10px;">
ABC Classification Filter
</h3>
""", unsafe_allow_html=True)

b1, b2, b3, b4 = st.columns(4)

with b1:
    if st.button("📦 Total SKUs", use_container_width=True, type="primary" if st.session_state.abc_filter == "Total SKUs" else "secondary"):
        st.session_state.abc_filter = "Total SKUs"

with b2:
    if st.button("🟢 Class A", use_container_width=True, type="primary" if st.session_state.abc_filter == "A" else "secondary"):
        st.session_state.abc_filter = "A"

with b3:
    if st.button("🟡 Class B", use_container_width=True, type="primary" if st.session_state.abc_filter == "B" else "secondary"):
        st.session_state.abc_filter = "B"

with b4:
    if st.button("🔴 Class C", use_container_width=True, type="primary" if st.session_state.abc_filter == "C" else "secondary"):
        st.session_state.abc_filter = "C"

st.markdown(
    f"""
    <div style="background:linear-gradient(135deg,#ffffff 0%,#f3fbfb 100%);
    padding:16px 22px;border-radius:16px;border-left:6px solid #008080;
    margin-top:18px;margin-bottom:22px;box-shadow:0 8px 20px rgba(6,18,67,.08);
    color:#061243;font-weight:800;font-size:18px;">
        Current View: <span style="color:#008080;">{st.session_state.abc_filter}</span>
    </div>
    """,
    unsafe_allow_html=True
)

# ================= GLOBAL KPI TABLE =================

global_kpis_df = build_global_kpi_comparison(st.session_state.abc_filter)
render_global_kpi_table(global_kpis_df)

st.subheader(f"SKU: {selected_sku} | Policy: {policy_name}")

# ================= KPI CARDS =================

if not sku_kpis.empty:
    row = sku_kpis.iloc[0]

    # ===== PRIMEIRA LINHA =====
    cols = st.columns(5)

    metrics = [
        ("Total Cost", ""),
        ("Stock Out Rate (%)", "%"),
        ("Alpha Service Level (%)", "%"),
        ("Beta Service Level (%)", "%"),
        ("Stock Coverage (days)", ""),
    ]

    for col, (metric, suffix) in zip(cols, metrics):
        value = pd.to_numeric(row.get(metric, 0), errors="coerce")
        value = 0 if pd.isna(value) else value
        col.metric(metric, f"{value:,.2f}{suffix}")

    # ===== SEGUNDA LINHA (COM ABC + XYZ) =====
    col1, col2, col3 = st.columns(3)

    with col1:
        value = pd.to_numeric(row.get("Average Inventory Level", 0), errors="coerce")
        value = 0 if pd.isna(value) else value
        st.metric("Average Inventory Level", f"{value:,.2f}")

    with col2:
        abc_value = row.get("ABC Class")

        if pd.isna(abc_value) or abc_value == "":
            abc_value = "N/A"
        else:
            abc_value = str(abc_value)
        st.markdown(f"""
        <div data-testid="stMetric">
            <div data-testid="stMetricLabel">ABC Class</div>
            <div data-testid="stMetricValue">{abc_value}</div>
        </div>
        """, unsafe_allow_html=True)

    with col3:
        xyz_value = str(row.get("XYZ Class", "N/A"))
        st.markdown(f"""
        <div data-testid="stMetric">
            <div data-testid="stMetricLabel">XYZ Class</div>
            <div data-testid="stMetricValue">{xyz_value}</div>
        </div>
        """, unsafe_allow_html=True)

else:
    st.warning("No KPI data found for this SKU.")

# ================= CHART =================

if not sku_df.empty and {"date", "demand", "soh_final"}.issubset(sku_df.columns):
    chart_long = sku_df[["date", "demand", "soh_final"]].melt(
        id_vars="date",
        value_vars=["demand", "soh_final"],
        var_name="Metric",
        value_name="Value"
    )

    chart_long["Metric"] = chart_long["Metric"].replace({
        "demand": "Demand",
        "soh_final": "Stock On Hand" if policy_name == "As Is" else "SOH Final"
    })

    fig = px.line(
        chart_long,
        x="date",
        y="Value",
        color="Metric",
        markers=False,
        title="Demand vs Stock On Hand Over Time" if policy_name == "As Is" else "Demand vs SOH Final Over Time",
        color_discrete_sequence=["#008080", "#061243"]
    )

    fig.update_layout(
        xaxis_title="Date",
        yaxis_title="Demand / Stock On Hand" if policy_name == "As Is" else "Demand / SOH Final",
        legend_title="Metric",
        hovermode="x unified",
        paper_bgcolor="white",
        plot_bgcolor="white",
        title_font=dict(size=22, color="#061243"),
        font=dict(color="#061243")
    )

    st.plotly_chart(fig, use_container_width=True)

else:
    st.info("No simulation time series available for this policy.")

# ================= KPI TABLE =================

st.subheader("KPI Table")

if not sku_kpis.empty:
    st.markdown(
        '<div class="chart-container"><div class="chart-frame-title">Selected SKU KPI Table</div>',
        unsafe_allow_html=True
    )

    st.dataframe(
        sku_kpis.drop(columns=["sku"], errors="ignore")
        .T.rename(columns={sku_kpis.index[0]: "Value"}),
        use_container_width=True
    )

    st.markdown('</div>', unsafe_allow_html=True)

# ================= RAW DATA =================

with st.expander("Show simulation data"):
    if not sku_df.empty:
        st.markdown(
            '<div class="chart-container"><div class="chart-frame-title">Simulation Raw Data</div>',
            unsafe_allow_html=True
        )

        st.dataframe(sku_df, use_container_width=True)

        st.markdown('</div>', unsafe_allow_html=True)
    else:
        st.info("No simulation data available for this policy.")

# ================= MONTE CARLO ANALYSIS =================

render_monte_carlo_analysis()
