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


/* ================= PREMIUM FRAMES ================= */
.chart-container{
    background: linear-gradient(180deg, #ffffff 0%, #f7fbfb 100%);
    padding: 24px 26px 18px 26px;
    border-radius: 26px;
    border: 1px solid rgba(6,18,67,.10);
    box-shadow: 0 14px 34px rgba(6,18,67,.10), 0 2px 8px rgba(6,18,67,.05);
    margin-bottom: 28px;
}
.chart-container:hover{
    box-shadow: 0 18px 42px rgba(6,18,67,.15), 0 4px 12px rgba(6,18,67,.08);
}
.chart-frame-title{
    color:#061243;
    font-size:18px;
    font-weight:900;
    margin-bottom:4px;
}
.chart-frame-subtitle{
    color:#008080;
    font-size:13px;
    font-weight:700;
    margin-bottom:12px;
}
.robustness-info-box{
    background: linear-gradient(135deg, #ffffff 0%, #f3fbfb 100%);
    border-left: 6px solid #008080;
    border-radius: 18px;
    padding: 14px 20px;
    margin: 10px 0 22px 0;
    box-shadow: 0 8px 22px rgba(6,18,67,.08);
    color:#061243;
    font-size:14px;
    font-weight:700;
}
div[data-testid="stExpander"]{
    background: linear-gradient(180deg,#ffffff 0%,#f8fcfc 100%) !important;
    border-radius:20px !important;
    border:1px solid rgba(6,18,67,.10) !important;
    overflow:hidden !important;
    box-shadow:0 10px 26px rgba(6,18,67,.08) !important;
    margin-bottom:16px !important;
}
div[data-testid="stExpander"] details summary{
    padding:14px 18px !important;
    font-weight:850 !important;
    color:#061243 !important;
}
.stDataFrame{
    border-radius:22px !important;
    overflow:hidden !important;
    border:1px solid rgba(6,18,67,.10) !important;
    box-shadow:0 12px 28px rgba(6,18,67,.08) !important;
    background:white !important;
}

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
    "Stock Cost",
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
        "Stockout Rate": "Stock Out Rate (%)",
        "Alpha Service Level": "Alpha Service Level (%)",
        "Beta Service Level": "Beta Service Level (%)",
        "total_holding_cost": "Stock Cost",
        "stock_out_rate_pct": "Stock Out Rate (%)",
        "stock_out_rate_%": "Stock Out Rate (%)",
        "stock_out_rate": "Stock Out Rate (%)",
        "alpha_service_level": "Alpha Service Level (%)",
        "alpha_service_level_%": "Alpha Service Level (%)",
        "beta_service_level": "Beta Service Level (%)",
        "beta_service_level_%": "Beta Service Level (%)",
        "average_inventory_level": "Average Inventory Level",
        "average_inventory_level_quantidade": "Average Inventory Level",
        "stock_coverage_days": "Stock Coverage (days)",
        "stock_coverage_dias": "Stock Coverage (days)",
        "custo_stock_total": "Stock Cost",
        "stock_cost": "Stock Cost",
    }

    df = df.rename(columns=rename_map)
    cols = ["sku", "ABC Class"] + KPI_ORDER
    return df[[c for c in cols if c in df.columns]]


def build_global_kpi_comparison(abc_filter="Total SKUs"):
    rows = []
    allowed_skus = None

    if abc_filter != "Total SKUs":
        asis_path = os.path.join(FOLDER, POLICIES["As Is"][1])
        asis_df = normalize_kpis(load_csv(asis_path))

        allowed_skus = asis_df.loc[
            asis_df["ABC Class"].astype(str).str.upper() == abc_filter,
            "sku"
        ].dropna().unique()

    for policy, (_, kpis_file) in POLICIES.items():
        path = os.path.join(FOLDER, kpis_file)

        if not os.path.exists(path):
            continue

        df = normalize_kpis(load_csv(path))

        if allowed_skus is not None:
            df = df[df["sku"].isin(allowed_skus)]

        if df.empty:
            continue

        values = {
            "Stock Cost": df["Stock Cost"].sum(),
            "Stock Out Rate (%)": df["Stock Out Rate (%)"].mean(),
            "Alpha Service Level (%)": df["Alpha Service Level (%)"].mean(),
            "Beta Service Level (%)": df["Beta Service Level (%)"].mean(),
            "Average Inventory Level": df["Average Inventory Level"].mean(),
            "Stock Coverage (days)": df["Stock Coverage (days)"].mean(),
        }

        rows += [{"KPI": k, "Policy": policy, "Value": round(v, 2)} for k, v in values.items()]

    pivot = pd.DataFrame(rows).pivot(index="KPI", columns="Policy", values="Value").reset_index()
    pivot["KPI"] = pd.Categorical(pivot["KPI"], categories=KPI_ORDER, ordered=True)

    return pivot.sort_values("KPI")


def render_global_kpi_table(df):
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
        <div class="global-kpi-title">Global KPIs by Policy</div>
        <table class="global-kpi-table">
            <thead><tr>{header}</tr></thead>
            <tbody>{body}</tbody>
        </table>
    </div>
    """, unsafe_allow_html=True)



def render_monte_carlo_robustness():
    """Render a fixed Monte Carlo robustness section for the Order Cycle Policy.

    This section is intentionally independent from the selected policy and SKU.
    It reads the Monte Carlo outputs generated by CicloDeEncomenda_COM_APENAS_MONTECARLO_ACRESCENTADO.py.
    """
    st.markdown("---")
    st.subheader("Monte Carlo Analysis - Order Cycle Policy")

    mc_summary_path = os.path.join(
        FOLDER,
        "PolíticaCicloDeEncomenda_MonteCarlo_FixedPolicy_Summary_by_Simulation.csv"
    )
    mc_target_path = os.path.join(
        FOLDER,
        "PolíticaCicloDeEncomenda_MonteCarlo_FixedPolicy_ServiceTarget.csv"
    )
    mc_robustness_path = os.path.join(
        FOLDER,
        "PolíticaCicloDeEncomenda_MonteCarlo_FixedPolicy_Robustness.csv"
    )

    if not os.path.exists(mc_summary_path):
        st.warning(
            "Monte Carlo summary file not found. Run the Order Cycle script with Monte Carlo first.\n\n"
            f"Expected file: {mc_summary_path}"
        )
        return

    mc_summary = load_csv(mc_summary_path)

    required_cols = [
        "Simulation",
        "Stock_Cost",
        "Stockout_Rate",
        "Beta_Service_Level",
        "Average_Inventory_Level",
    ]

    missing_cols = [col for col in required_cols if col not in mc_summary.columns]
    if missing_cols:
        st.error(
            "Monte Carlo summary file exists, but these required columns are missing: "
            + ", ".join(missing_cols)
        )
        return

    for col in [
        "Stock_Cost",
        "Stockout_Rate",
        "Alpha_Service_Level",
        "Beta_Service_Level",
        "Average_Inventory_Level",
        "Stock_Coverage_Days",
        "Service_Level_Target",
    ]:
        if col in mc_summary.columns:
            mc_summary[col] = pd.to_numeric(mc_summary[col], errors="coerce")

    target_service = 95.0
    probability_beta_target = None

    if os.path.exists(mc_target_path):
        mc_target = load_csv(mc_target_path)
        if not mc_target.empty:
            if "Service Target (%)" in mc_target.columns:
                target_values = pd.to_numeric(mc_target["Service Target (%)"], errors="coerce").dropna()
                if not target_values.empty:
                    target_service = float(target_values.iloc[0])
            if "Probability Beta Service Level >= Target (%)" in mc_target.columns:
                prob_values = pd.to_numeric(
                    mc_target["Probability Beta Service Level >= Target (%)"],
                    errors="coerce"
                ).dropna()
                if not prob_values.empty:
                    probability_beta_target = float(prob_values.iloc[0])

    if probability_beta_target is None:
        probability_beta_target = mc_summary["Beta_Service_Level"].ge(target_service).mean() * 100

    mean_beta = mc_summary["Beta_Service_Level"].mean()
    mean_stock_cost = mc_summary["Stock_Cost"].mean()
    worst_stockout = mc_summary["Stockout_Rate"].max()

    beta_min = mc_summary["Beta_Service_Level"].min()
    beta_max = mc_summary["Beta_Service_Level"].max()
    beta_padding = max(0.05, (beta_max - beta_min) * 0.25)

    cost_min = mc_summary["Stock_Cost"].min()
    cost_max = mc_summary["Stock_Cost"].max()
    cost_padding = max(1.0, (cost_max - cost_min) * 0.08)

    navy = "#061243"
    teal = "#008080"
    teal_light = "#8ed6d6"
    orange = "#ff7f32"
    grid = "rgba(6,18,67,0.12)"

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Mean β Service Level", f"{mean_beta:.2f}%")
    c2.metric(f"Probability β SL ≥ {target_service:.2f}%", f"{probability_beta_target:.2f}%")
    c3.metric("Mean Stock Cost", f"{mean_stock_cost:,.2f}")
    c4.metric("Worst Stockout Rate", f"{worst_stockout:.2f}%")

    chart_col_1, chart_col_2 = st.columns(2)

    with chart_col_1:
        fig_beta = px.histogram(
            mc_summary,
            x="Beta_Service_Level",
            nbins=15,
            title="Distribution of β Service Level",
            color_discrete_sequence=[teal],
            hover_data={
                "Simulation": True,
                "Beta_Service_Level": ":,.2f",
                "Stockout_Rate": ":,.2f",
            },
        )

        # The target line may be outside the zoomed range when the policy is very robust.
        fig_beta.add_vline(
            x=target_service,
            line_dash="dash",
            line_color=orange,
            line_width=2,
            annotation_text=f"Target: {target_service:.2f}%",
            annotation_position="top left",
        )

        fig_beta.add_vline(
            x=mean_beta,
            line_dash="dash",
            line_color=navy,
            line_width=2,
            annotation_text=f"Mean: {mean_beta:.2f}%",
            annotation_position="top right",
        )

        fig_beta.update_traces(
            marker_line_color="white",
            marker_line_width=1.2,
            opacity=0.92,
        )

        fig_beta.update_layout(
            xaxis_title="β Service Level (%)",
            yaxis_title="Number of Simulations",
            height=520,
            paper_bgcolor="white",
            plot_bgcolor="white",
            title_font=dict(size=20, color=navy),
            font=dict(color=navy),
            bargap=0.08,
            margin=dict(l=60, r=35, t=80, b=70),
            xaxis=dict(
                range=[beta_min - beta_padding, beta_max + beta_padding],
                showgrid=True,
                gridcolor=grid,
                zeroline=False,
            ),
            yaxis=dict(
                showgrid=True,
                gridcolor=grid,
                zeroline=False,
            ),
        )

        if target_service < beta_min - beta_padding:
            fig_beta.add_annotation(
                x=beta_min,
                y=1,
                xref="x",
                yref="paper",
                text=f"Target {target_service:.2f}% is below this zoomed range",
                showarrow=False,
                font=dict(color=orange, size=11),
                align="left",
                bgcolor="rgba(255,255,255,0.85)",
                bordercolor=orange,
                borderwidth=1,
            )

        st.plotly_chart(fig_beta, use_container_width=True)

    with chart_col_2:
        fig_cost = px.histogram(
            mc_summary,
            x="Stock_Cost",
            nbins=20,
            title="Distribution of Stock Cost",
            color_discrete_sequence=[navy],
            hover_data={
                "Simulation": True,
                "Stock_Cost": ":,.2f",
                "Average_Inventory_Level": ":,.2f",
            },
        )

        fig_cost.add_vline(
            x=mean_stock_cost,
            line_dash="dash",
            line_color=teal,
            line_width=2,
            annotation_text=f"Mean: {mean_stock_cost:,.2f}",
            annotation_position="top left",
        )

        fig_cost.update_traces(
            marker_line_color="white",
            marker_line_width=1.2,
            opacity=0.92,
        )

        fig_cost.update_layout(
            xaxis_title="Stock Cost",
            yaxis_title="Number of Simulations",
            height=520,
            paper_bgcolor="white",
            plot_bgcolor="white",
            title_font=dict(size=20, color=navy),
            font=dict(color=navy),
            bargap=0.08,
            margin=dict(l=60, r=35, t=80, b=70),
            xaxis=dict(
                range=[cost_min - cost_padding, cost_max + cost_padding],
                showgrid=True,
                gridcolor=grid,
                zeroline=False,
            ),
            yaxis=dict(
                showgrid=True,
                gridcolor=grid,
                zeroline=False,
            ),
        )

        st.plotly_chart(fig_cost, use_container_width=True)

    with st.expander("Show optional Stock Cost vs β Service Level scatter"):
        fig_mc = px.scatter(
            mc_summary,
            x="Stock_Cost",
            y="Beta_Service_Level",
            color="Average_Inventory_Level",
            hover_name="Simulation",
            title="Monte Carlo Scenario Dispersion: Stock Cost vs β Service Level",
            color_continuous_scale=[[0, teal_light], [0.5, teal], [1, navy]],
            hover_data={
                "Stock_Cost": ":,.2f",
                "Beta_Service_Level": ":,.2f",
                "Stockout_Rate": ":,.2f",
                "Average_Inventory_Level": ":,.2f",
            },
        )

        fig_mc.add_hline(
            y=target_service,
            line_dash="dash",
            line_color=orange,
            annotation_text=f"Target β SL: {target_service:.2f}%",
            annotation_position="top left",
        )

        fig_mc.update_traces(
            marker=dict(size=8, line=dict(width=0.8, color="white")),
            opacity=0.88,
        )

        fig_mc.update_layout(
            xaxis_title="Stock Cost",
            yaxis_title="β Service Level (%)",
            height=560,
            paper_bgcolor="white",
            plot_bgcolor="white",
            title_font=dict(size=20, color=navy),
            font=dict(color=navy),
            coloraxis_colorbar=dict(title="Avg Inventory"),
            xaxis=dict(showgrid=True, gridcolor=grid, zeroline=False),
            yaxis=dict(
                range=[beta_min - beta_padding, beta_max + beta_padding],
                showgrid=True,
                gridcolor=grid,
                zeroline=False,
            ),
        )

        st.plotly_chart(fig_mc, use_container_width=True)

    if os.path.exists(mc_robustness_path):
        with st.expander("Show Monte Carlo robustness statistics"):
            robustness_df = load_csv(mc_robustness_path)
            st.markdown('<div class="chart-container"><div class="chart-frame-title">Monte Carlo Robustness Statistics</div>', unsafe_allow_html=True)
            st.dataframe(robustness_df, use_container_width=True)
            st.markdown('</div>', unsafe_allow_html=True)

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
        available_skus = sorted(
            set(df["sku"].dropna().astype(str)).intersection(valid_asis_skus)
        )
    else:
        available_skus = sorted(df["sku"].dropna().astype(str).unique())

else:
    available_skus = sorted(df["sku"].dropna().astype(str).unique())

if not available_skus:
    st.error("No valid SKUs available for the selected policy.")
    st.stop()

selected_sku = st.sidebar.selectbox("Select SKU", available_skus)

sku_kpis = sku_kpis_all[sku_kpis_all["sku"] == selected_sku]
sku_df = df[df["sku"] == selected_sku].sort_values("date")

# ================= DASHBOARD =================

st.markdown("""
<style>
div.stButton > button {
    width: 100%;
    height: 58px;
    border-radius: 16px;
    border: none;
    background: linear-gradient(135deg, #061243 0%, #008080 100%);
    color: white;
    font-size: 18px;
    font-weight: 800;
    box-shadow: 0 8px 20px rgba(6,18,67,.18);
    transition: all 0.25s ease;
}

div.stButton > button:hover {
    transform: translateY(-3px);
    box-shadow: 0 12px 24px rgba(6,18,67,.28);
    background: linear-gradient(135deg, #008080 0%, #061243 100%);
    color: white;
}

div.stButton > button:focus {
    border: 2px solid #00b3b3 !important;
    color: white !important;
}

div.stButton > button[kind="primary"] {
    background: linear-gradient(135deg, #00a6a6 0%, #061243 100%);
    border: 2px solid #00d4d4;
}
</style>
""", unsafe_allow_html=True)

if "abc_filter" not in st.session_state:
    st.session_state.abc_filter = "Total SKUs"

st.markdown("""
<h3 style="
color:#061243;
font-weight:900;
margin-bottom:18px;
margin-top:10px;">
ABC Classification Filter
</h3>
""", unsafe_allow_html=True)

b1, b2, b3, b4 = st.columns(4)

with b1:
    if st.button(
        "📦 Total SKUs",
        use_container_width=True,
        type="primary" if st.session_state.abc_filter == "Total SKUs" else "secondary"
    ):
        st.session_state.abc_filter = "Total SKUs"

with b2:
    if st.button(
        "🟢 Class A",
        use_container_width=True,
        type="primary" if st.session_state.abc_filter == "A" else "secondary"
    ):
        st.session_state.abc_filter = "A"

with b3:
    if st.button(
        "🟡 Class B",
        use_container_width=True,
        type="primary" if st.session_state.abc_filter == "B" else "secondary"
    ):
        st.session_state.abc_filter = "B"

with b4:
    if st.button(
        "🔴 Class C",
        use_container_width=True,
        type="primary" if st.session_state.abc_filter == "C" else "secondary"
    ):
        st.session_state.abc_filter = "C"

st.markdown(
    f"""
    <div style="
        background: linear-gradient(135deg, #ffffff 0%, #f3fbfb 100%);
        padding:16px 22px;
        border-radius:16px;
        border-left:6px solid #008080;
        margin-top:18px;
        margin-bottom:22px;
        box-shadow:0 8px 20px rgba(6,18,67,.08);
        color:#061243;
        font-weight:800;
        font-size:18px;">
        Current View: <span style="color:#008080;">{st.session_state.abc_filter}</span>
    </div>
    """,
    unsafe_allow_html=True
)

global_kpis_df = build_global_kpi_comparison(st.session_state.abc_filter)

render_global_kpi_table(global_kpis_df)

st.subheader(f"SKU: {selected_sku} | Policy: {policy_name}")

# ================= KPI CARDS =================

if not sku_kpis.empty:
    row = sku_kpis.iloc[0]
    cols = st.columns(5)

    metrics = [
        ("Stock Cost", ""),
        ("Stock Out Rate (%)", "%"),
        ("Alpha Service Level (%)", "%"),
        ("Beta Service Level (%)", "%"),
        ("Stock Coverage (days)", ""),
    ]

    for col, (metric, suffix) in zip(cols, metrics):
        col.metric(metric, f"{row.get(metric, 0):,.2f}{suffix}")

    st.metric(
        "Average Inventory Level",
        f"{row.get('Average Inventory Level', 0):,.2f}"
    )

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
        title="Demand vs Stock On Hand Over Time"
        if policy_name == "As Is"
        else "Demand vs SOH Final Over Time",
        color_discrete_sequence=["#008080", "#061243"]
    )

    fig.update_layout(
        xaxis_title="Date",
        yaxis_title="Demand / Stock On Hand"
        if policy_name == "As Is"
        else "Demand / SOH Final",
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
    st.markdown('<div class="chart-container"><div class="chart-frame-title">Selected SKU KPI Table</div>', unsafe_allow_html=True)
    st.dataframe(
        sku_kpis.drop(columns=["sku"], errors="ignore")
        .T.rename(columns={sku_kpis.index[0]: "Value"}),
        use_container_width=True
    )
    st.markdown('</div>', unsafe_allow_html=True)

# ================= RAW DATA =================

with st.expander("Show simulation data"):

    if not sku_df.empty:
        st.markdown('<div class="chart-container"><div class="chart-frame-title">Simulation Raw Data</div>', unsafe_allow_html=True)
        st.dataframe(sku_df, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)
    else:
        st.info("No simulation data available for this policy.")

# ================= MONTE CARLO ROBUSTNESS =================

render_monte_carlo_robustness()
