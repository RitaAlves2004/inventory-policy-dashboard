import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import os

st.set_page_config(page_title="Forecast Alerts", layout="wide")

# ================= STYLE =================

st.markdown("""
<style>
    .stApp {
        background: linear-gradient(180deg, #f8fafc 0%, #eef3f8 100%);
    }

    section[data-testid="stSidebar"] {
        background: linear-gradient(180deg, #07113f 0%, #0b1f5c 100%);
    }

    section[data-testid="stSidebar"] h1,
    section[data-testid="stSidebar"] h2,
    section[data-testid="stSidebar"] h3,
    section[data-testid="stSidebar"] label,
    section[data-testid="stSidebar"] p,
    section[data-testid="stSidebar"] span {
        color: white !important;
    }

    section[data-testid="stSidebar"] div[data-baseweb="select"] span {
        color: #061243 !important;
    }

    .block-container {
        padding-top: 2rem;
        padding-bottom: 4rem;
    }

    .dashboard-header {
        background: white;
        border-radius: 22px;
        padding: 28px 34px;
        margin-bottom: 28px;
        border: 1px solid rgba(6, 18, 67, 0.12);
        box-shadow: 0 10px 28px rgba(6, 18, 67, 0.08);
    }

    .dashboard-title {
        color: #061243;
        font-size: 48px;
        font-weight: 900;
        letter-spacing: -1px;
        margin-bottom: 0;
    }

    .dashboard-subtitle {
        color: #008080;
        font-size: 18px;
        font-weight: 700;
        margin-top: 4px;
    }

    h1, h2, h3 {
        color: #061243;
        font-weight: 800 !important;
    }

    div[data-testid="stMetric"] {
        background: white;
        border: 1px solid rgba(6, 18, 67, 0.12);
        border-left: 6px solid #008080;
        padding: 18px 20px;
        border-radius: 18px;
        box-shadow: 0 8px 22px rgba(6, 18, 67, 0.08);
    }

    div[data-testid="stMetricLabel"] {
        color: #061243 !important;
        font-weight: 700;
    }

    div[data-testid="stMetricValue"] {
        color: #061243 !important;
        font-size: 32px !important;
        font-weight: 850 !important;
    }

    .stDataFrame {
        background: white;
        border-radius: 16px;
        box-shadow: 0 8px 20px rgba(6, 18, 67, 0.08);
    }

    div[data-testid="stExpander"] {
        background: white;
        border-radius: 16px;
        border: 1px solid rgba(6, 18, 67, 0.12);
        box-shadow: 0 6px 18px rgba(6, 18, 67, 0.06);
    }
</style>
""", unsafe_allow_html=True)

# ================= LOGOS =================

LOGO_1 = "Uni_Logo.png"
LOGO_2 = "LTP_Logo.png"

st.markdown('<div class="dashboard-header">', unsafe_allow_html=True)

col_logo1, col_logo2, col_space = st.columns([1, 1, 7])

with col_logo1:
    st.image(LOGO_1, width=105)

with col_logo2:
    st.image(LOGO_2, width=125)

with col_space:
    st.markdown(
        """
        <div class="dashboard-title">Forecast Error Monitoring</div>
        <div class="dashboard-subtitle">Forecast Alerts and Accuracy Dashboard</div>
        """,
        unsafe_allow_html=True
    )

st.markdown('</div>', unsafe_allow_html=True)

# ================= CONFIG =================

FOLDER = "."

FORECAST_CSV = os.path.join(
    FOLDER,
    "forecast_simulation_lgbm_daily.csv"
)

KPI_CSV = os.path.join(
    FOLDER,
    "forecast_kpis_lgbm_daily.csv")

# ================= LOAD KPI DATA =================

@st.cache_data
def load_kpis():

    return pd.read_csv(
        KPI_CSV,
        usecols=[
            "sku",
            "MAE",
            "RMSE",
            "MAPE (%)",
            "wMAPE (%)"
        ]
    )


kpi_df = load_kpis()

# ================= SIDEBAR =================

st.sidebar.title("Filters")

# ================= VALID SKUS =================

@st.cache_data
def get_valid_skus():

    valid_skus = set()

    for chunk in pd.read_csv(
        FORECAST_CSV,
        usecols=["sku", "demand"],
        chunksize=100000
    ):

        chunk = chunk[
            chunk["demand"] > 0
        ]

        valid_skus.update(
            chunk["sku"].dropna().unique()
        )

    return sorted(valid_skus)


available_skus = get_valid_skus()

selected_sku = st.sidebar.selectbox(
    "Select SKU",
    available_skus
)

# ================= LOAD SKU DATA ONLY =================

@st.cache_data
def load_forecast_for_sku(selected_sku):

    chunks = []

    for chunk in pd.read_csv(
        FORECAST_CSV,
        usecols=["sku", "date", "demand", "forecast"],
        chunksize=100000
    ):

        filtered = chunk[
            chunk["sku"] == selected_sku
        ]

        if not filtered.empty:
            chunks.append(filtered)

    if chunks:
        df = pd.concat(
            chunks,
            ignore_index=True
        )
    else:
        df = pd.DataFrame()

    df["date"] = pd.to_datetime(
        df["date"].astype(str),
        format="%Y%m%d",
        errors="coerce"
    )

    return df


sku_df = load_forecast_for_sku(selected_sku)

# ================= CALCULATIONS =================

sku_df = sku_df.sort_values("date")

sku_df["error"] = (
    sku_df["demand"] -
    sku_df["forecast"]
)

mean_error = sku_df["error"].mean()

std_error = sku_df["error"].std()

upper_limit = mean_error + (2 * std_error)

lower_limit = mean_error - (2 * std_error)

sku_df["upper_limit"] = upper_limit

sku_df["lower_limit"] = lower_limit

sku_df["out_of_limits"] = (
    (sku_df["error"] > upper_limit) |
    (sku_df["error"] < lower_limit)
)

# ================= ALERT DETECTION =================

sku_df["alert"] = False

consecutive = 0

for i in sku_df.index:

    if sku_df.loc[i, "out_of_limits"]:
        consecutive += 1
    else:
        consecutive = 0

    if consecutive >= 3:
        sku_df.loc[i, "alert"] = True

# ================= PAGE =================

st.subheader(f"SKU: {selected_sku}")

# ================= ALERT MESSAGE =================

if sku_df["alert"].any():

    st.error(
        "⚠️ Three consecutive out-of-limit "
        "errors detected for this SKU."
    )

# ================= GRAPH =================

fig = go.Figure()

fig.add_trace(
    go.Scatter(
        x=sku_df["date"],
        y=sku_df["error"],
        mode="lines+markers",
        name="Forecast Error",
        line=dict(color="#061243", width=3),
        marker=dict(size=5),
    )
)

fig.add_trace(
    go.Scatter(
        x=sku_df["date"],
        y=sku_df["upper_limit"],
        mode="lines",
        name="Upper Limit",
        line=dict(
            color="#008080",
            dash="dash",
            width=2
        ),
    )
)

fig.add_trace(
    go.Scatter(
        x=sku_df["date"],
        y=sku_df["lower_limit"],
        mode="lines",
        name="Lower Limit",
        line=dict(
            color="#008080",
            dash="dash",
            width=2
        ),
    )
)

out_df = sku_df[
    sku_df["out_of_limits"]
]

fig.add_trace(
    go.Scatter(
        x=out_df["date"],
        y=out_df["error"],
        mode="markers",
        name="Out of Limits",
        marker=dict(
            color="#ff7f32",
            size=10
        )
    )
)

alert_df = sku_df[
    sku_df["alert"]
]

fig.add_trace(
    go.Scatter(
        x=alert_df["date"],
        y=alert_df["error"],
        mode="markers",
        name="Alert",
        marker=dict(
            color="red",
            size=13,
            symbol="diamond"
        )
    )
)

fig.update_layout(
    title="Forecast Error Control Chart",
    xaxis_title="Date",
    yaxis_title="Forecast Error",
    hovermode="x unified",
    height=700,
    paper_bgcolor="white",
    plot_bgcolor="white",
    title_font=dict(size=24, color="#061243"),
    font=dict(color="#061243"),
    legend=dict(
        orientation="h",
        yanchor="bottom",
        y=-0.22,
        xanchor="center",
        x=0.5
    ),
    margin=dict(l=70, r=70, t=90, b=100)
)

st.plotly_chart(
    fig,
    use_container_width=True
)

# ================= KPI SUMMARY =================

st.subheader("Forecast KPIs")

sku_kpi = kpi_df[
    kpi_df["sku"] == selected_sku
]

if not sku_kpi.empty:

    row = sku_kpi.iloc[0]

    col1, col2, col3, col4 = st.columns(4)

    col1.metric(
        "MAE",
        f"{row['MAE']:,.2f}"
    )

    col2.metric(
        "RMSE",
        f"{row['RMSE']:,.2f}"
    )

    col3.metric(
        "MAPE (%)",
        f"{row['MAPE (%)']:,.2f}"
    )

    col4.metric(
        "wMAPE (%)",
        f"{row['wMAPE (%)']:,.2f}"
    )

else:
    st.warning(
        "No KPI data found."
    )

# ================= RAW DATA =================

with st.expander("Show Data"):

    st.dataframe(
        sku_df,
        use_container_width=True
    )
