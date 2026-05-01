import os
import pandas as pd
import numpy as np
from collections import defaultdict
from statistics import NormalDist

folder = r"C:\Users\madel\OneDrive\Ambiente de Trabalho\4 ano\GCA\Projeto 2026\parquet_filtered_final"

start_date = pd.Timestamp("2023-06-01")
service_level_candidates = np.round(np.arange(0.90, 0.991, 0.01), 2)
z_candidates = {sl: NormalDist().inv_cdf(sl) for sl in service_level_candidates}

transport_cost_per_unit = 0.15
WACC = 0.02

stock_file = "master_stock_forecast.parquet"
review_file = "20260210_review_days.parquet"
leadtime_file = "master_lead_times.parquet"
cost_file = "20260210_CustosProdutos.xlsx"
policy_file = "20260210_stock_policy_parameters.parquet"

output_file = "PolíticaNívelDeEncomenda.csv"

def normalize_sku(s):
    return s.astype(str).str.strip().str.upper().str.replace(r"\s+", "", regex=True)

def current_day_custom(s):
    return ((s.dt.weekday + 1) % 7) + 1

def make_forward_sum(values):
    return np.r_[0, np.cumsum(np.asarray(values, dtype=float))]

def sum_forward_prefix(prefix, start_idx, window):
    if window <= 0 or start_idx >= len(prefix) - 1:
        return 0.0
    end_idx = min(start_idx + window, len(prefix) - 1)
    return float(prefix[end_idx] - prefix[start_idx])

# ========================= LER E NORMALIZAR =========================

stock = pd.read_parquet(os.path.join(folder, stock_file))
review = pd.read_parquet(os.path.join(folder, review_file))
leadtimes = pd.read_parquet(os.path.join(folder, leadtime_file))
costs = pd.read_excel(os.path.join(folder, cost_file))
policy = pd.read_parquet(os.path.join(folder, policy_file))

for df in [stock, review, leadtimes, costs, policy]:
    df["sku"] = normalize_sku(df["sku"])

# ========================= STOCK =========================

stock["date"] = pd.to_datetime(stock["date"].astype(str), format="%Y%m%d", errors="coerce")

for col in ["forecast", "demand", "stock_on_hand"]:
    stock[col] = pd.to_numeric(stock[col], errors="coerce").fillna(0)

stock = stock.loc[
    stock["date"].ge(start_date),
    ["sku", "date", "forecast", "demand", "stock_on_hand"]
].dropna(subset=["sku", "date"])

sku_activity = stock.groupby("sku", as_index=False).agg(
    total_forecast=("forecast", "sum"),
    total_demand=("demand", "sum"))

valid_skus = sku_activity.loc[
    (sku_activity["total_forecast"] != 0) | (sku_activity["total_demand"] != 0),
    "sku"]

stock = stock[stock["sku"].isin(valid_skus)].copy()
stock["forecast_error"] = stock["demand"] - stock["forecast"]

# ========================= REVIEW DAYS =========================

review["review_day"] = pd.to_numeric(review["review_day"], errors="coerce")

review_clean = review.dropna(subset=["sku", "review_day"]).copy()
review_clean["review_day"] = review_clean["review_day"].astype(int)

review_days_map = (
    review_clean.groupby("sku")["review_day"]
    .apply(lambda x: set(x.unique()))
    .to_dict())

all_skus = set(stock["sku"].unique())

for sku in all_skus:
    if sku not in review_days_map or not review_days_map[sku]:
        review_days_map[sku] = {2}

review_days_str = pd.DataFrame({
    "sku": list(review_days_map.keys()),
    "review_days": [
        ",".join(map(str, sorted(days)))
        for days in review_days_map.values()]})

# ========================= LEAD TIMES =========================

for col in ["avg_lead_time_real", "std_lead_time_real"]:
    leadtimes[col] = pd.to_numeric(
        leadtimes[col].astype(str).str.replace(",", ".", regex=False).str.strip(),
        errors="coerce")

leadtimes = (
    leadtimes[["sku", "avg_lead_time_real", "std_lead_time_real"]]
    .dropna(subset=["sku", "avg_lead_time_real"])
    .groupby("sku", as_index=False)
    .agg(
        avg_lead_time_real=("avg_lead_time_real", "mean"),
        std_lead_time_real=("std_lead_time_real", "mean")))

leadtimes["std_lead_time_real"] = leadtimes["std_lead_time_real"].fillna(0)
leadtimes = leadtimes[leadtimes["avg_lead_time_real"] > 0]

# ========================= CUSTOS E MOQ =========================

costs["custo"] = pd.to_numeric(costs["custo"], errors="coerce").fillna(0)

costs = (
    costs[["sku", "custo"]]
    .dropna(subset=["sku"])
    .groupby("sku", as_index=False)
    .agg(unit_cost=("custo", "mean")))

costs["transport_cost_per_unit"] = transport_cost_per_unit
costs["total_unit_cost"] = costs["unit_cost"] + costs["transport_cost_per_unit"]

policy["moq_units"] = pd.to_numeric(policy["moq_units"], errors="coerce").fillna(1)

policy = (
    policy[["sku", "moq_units"]]
    .dropna(subset=["sku"])
    .groupby("sku", as_index=False)
    .agg(moq_units=("moq_units", "max")))

# ========================= STOCK DIÁRIO BASE =========================

stock_daily = (
    stock.groupby(["sku", "date"], as_index=False)
    .agg(
        forecast=("forecast", "sum"),
        demand=("demand", "sum"),
        stock_on_hand=("stock_on_hand", "max"),
        forecast_error=("forecast_error", "sum"))
    .sort_values(["sku", "date"]))

forecast_std = stock_daily.groupby("sku", as_index=False).agg(
    std_forecast=("forecast_error", "std"))

initial_stock = (
    stock_daily.sort_values(["sku", "date"])
    .groupby("sku", as_index=False)
    .first()[["sku", "stock_on_hand"]]
    .rename(columns={"stock_on_hand": "initial_soh"}))

stock_daily = (
    stock_daily
    .merge(forecast_std, on="sku", how="left")
    .merge(initial_stock, on="sku", how="left")
    .merge(leadtimes, on="sku", how="inner")
    .merge(costs, on="sku", how="left")
    .merge(policy, on="sku", how="left")
    .merge(review_days_str, on="sku", how="left"))

stock_daily = stock_daily.fillna({
    "std_forecast": 0,
    "initial_soh": 0,
    "unit_cost": 0,
    "transport_cost_per_unit": transport_cost_per_unit,
    "moq_units": 1,
    "review_days": ""})

stock_daily["moq_units"] = stock_daily["moq_units"].clip(lower=1)
stock_daily["total_unit_cost"] = stock_daily["unit_cost"] + stock_daily["transport_cost_per_unit"]
stock_daily["current_day"] = current_day_custom(stock_daily["date"])

stock_daily["is_review_day"] = [
    int(day) in review_days_map.get(sku, set())
    for sku, day in zip(stock_daily["sku"], stock_daily["current_day"])]

# ========================= SIMULAR SKU =========================

def simulate_sku_fast(sku, sku_df, service_level, return_rows=False):
    sku_df = sku_df.sort_values("date").reset_index(drop=True)

    z = z_candidates[service_level]

    dates = sku_df["date"].to_numpy()
    forecasts = sku_df["forecast"].to_numpy(float)
    demands = sku_df["demand"].to_numpy(float)
    avg_lt = sku_df["avg_lead_time_real"].to_numpy(float)
    std_lt = sku_df["std_lead_time_real"].to_numpy(float)
    sigma_daily_arr = sku_df["std_forecast"].to_numpy(float)

    initial_soh = float(sku_df.loc[0, "initial_soh"])
    unit_cost = sku_df["unit_cost"].to_numpy(float)
    total_unit_cost = sku_df["total_unit_cost"].to_numpy(float)
    moq_units = sku_df["moq_units"].to_numpy(float)
    is_review_day = sku_df["is_review_day"].to_numpy(bool)
    review_days = sku_df["review_days"].to_numpy()
    current_day = sku_df["current_day"].to_numpy(int)

    prefix_forecast = make_forward_sum(forecasts)

    avg_daily_demand = float(np.mean(forecasts))
    if avg_daily_demand <= 0:
        avg_daily_demand = float(np.mean(demands))
    avg_daily_demand = max(0.0, avg_daily_demand)

    n = len(sku_df)

    DDLT_mean = np.zeros(n)
    DDLT_sigma = np.zeros(n)
    safety_stock_SS = np.zeros(n)
    order_up_to_level_S = np.zeros(n)

    for i in range(n):
        lead_time = max(0.0, avg_lt[i])
        std_lead_time = max(0.0, std_lt[i])
        sigma_daily = max(0.0, sigma_daily_arr[i])
        lead_time_days = max(1, int(np.ceil(lead_time)))

        ddlt_mean = sum_forward_prefix(prefix_forecast, i, lead_time_days)
        if ddlt_mean <= 0:
            ddlt_mean = avg_daily_demand * lead_time

        ddlt_sigma = np.sqrt(max(
            0.0,
            lead_time * sigma_daily**2 + avg_daily_demand**2 * std_lead_time**2))

        safety_stock = z * ddlt_sigma

        DDLT_mean[i] = int(np.ceil(ddlt_mean))
        DDLT_sigma[i] = round(ddlt_sigma, 2)
        safety_stock_SS[i] = int(np.ceil(safety_stock))
        order_up_to_level_S[i] = int(np.ceil(ddlt_mean + safety_stock))

    scheduled_deliveries = defaultdict(float)
    soh_inicio = initial_soh

    rows = []

    stockout_total = 0.0
    inventory_value_total = 0.0
    order_total_cost_total = 0.0

    for i in range(n):
        date = pd.Timestamp(dates[i])

        delivered = float(scheduled_deliveries.get(date, 0.0))
        pipeline_open = sum(
            qty for delivery_date, qty in scheduled_deliveries.items()
            if delivery_date >= date)

        stock_position = soh_inicio + pipeline_open
        ordered = 0.0
        order_triggered = False

        if is_review_day[i] and stock_position < order_up_to_level_S[i]:
            lead_time_days = max(1, int(np.ceil(avg_lt[i])))
            need = order_up_to_level_S[i] - stock_position
            ordered = float(np.ceil(max(need, moq_units[i])))
            order_triggered = True

            delivery_date = date + pd.Timedelta(days=lead_time_days)
            scheduled_deliveries[delivery_date] += ordered

        available_today = soh_inicio + delivered
        stockout = max(0.0, demands[i] - available_today)
        soh_final = max(0.0, available_today - demands[i])

        inventory_value = soh_final * unit_cost[i]
        inventory_holding_cost_day = inventory_value * (WACC / 365)

        order_total_cost = ordered * total_unit_cost[i]

        stockout_total += stockout
        inventory_value_total += inventory_value
        order_total_cost_total += order_total_cost

        if return_rows:
            rows.append({
                "SKU": sku,
                "Date": date,
                "Forecast": int(round(forecasts[i])),
                "Demand": int(round(demands[i])),
                "Stock Position": int(round(stock_position)),
                "SOH Start": int(round(soh_inicio)),
                "Orders Placed": int(round(ordered)),
                "Orders Delivered": int(round(delivered)),
                "SOH End": int(round(soh_final)),
                "Stockout": int(round(stockout)),
                "Order Triggered": order_triggered,
                "Is Review Day": bool(is_review_day[i]),
                "MOQ": int(np.ceil(moq_units[i])),
                "Order Up To Level S": int(order_up_to_level_S[i]),
                "Review Days": review_days[i],
                "Current Day": int(current_day[i]),
                "SS": int(safety_stock_SS[i]),
                "DDLT mean": int(DDLT_mean[i]),
                "DDLT sigma": float(DDLT_sigma[i]),
                "Inventory Holding Cost": round(inventory_holding_cost_day, 6),
                "avg LT Real": round(avg_lt[i], 2),
                "std LT Real": round(std_lt[i], 2),
                "Alpha Service Level": float(service_level),
                "z": float(z)})

        soh_inicio = soh_final

    metrics = {
        "service_level": service_level,
        "z": z,
        "stockout_total": stockout_total,
        "inventory_value_total": inventory_value_total,
        "order_total_cost_total": order_total_cost_total}

    if return_rows:
        return pd.DataFrame(rows), metrics

    return None, metrics

# ========================= OTIMIZAÇÃO =========================

best_results = []
optimization_summary = []

for sku, sku_base_df in stock_daily.groupby("sku", sort=True):
    best_metrics = None

    for service_level in service_level_candidates:
        _, metrics = simulate_sku_fast(
            sku,
            sku_base_df,
            service_level,
            return_rows=False)

        if best_metrics is None or (
            metrics["stockout_total"],
            metrics["inventory_value_total"],
            metrics["order_total_cost_total"]
        ) < (
            best_metrics["stockout_total"],
            best_metrics["inventory_value_total"],
            best_metrics["order_total_cost_total"]):
            best_metrics = metrics

    best_simulation, _ = simulate_sku_fast(
        sku,
        sku_base_df,
        best_metrics["service_level"],
        return_rows=True)

    best_results.append(best_simulation)

    optimization_summary.append({
        "sku": sku,
        "optimal_service_level": best_metrics["service_level"],
        "optimal_z": best_metrics["z"],
        "stockout_total": best_metrics["stockout_total"],
        "inventory_value_total": best_metrics["inventory_value_total"],
        "order_total_cost_total": best_metrics["order_total_cost_total"]})

# ========================= DATAFRAME FINAL =========================

final_df = pd.concat(best_results, ignore_index=True)
optimization_summary_df = pd.DataFrame(optimization_summary)

numeric_cols_2_decimals = [
    "avg LT Real",
    "std LT Real",
    "DDLT sigma",
    "Inventory Holding Cost",
    "Alpha Service Level",
    "z"]

for col in numeric_cols_2_decimals:
    if col in final_df.columns:
        final_df[col] = pd.to_numeric(
            final_df[col],
            errors="coerce"
        ).round(4 if col in ["Alpha Service Level", "z"] else 2)

# ========================= BETA SERVICE LEVEL POR SKU =========================

beta_by_sku = (
    final_df.groupby("SKU", as_index=False)
    .agg(
        total_demand=("Demand", "sum"),
        total_stockout=("Stockout", "sum")))

beta_by_sku["Beta Service Level"] = np.where(
    beta_by_sku["total_demand"] > 0,
    (1 - beta_by_sku["total_stockout"] / beta_by_sku["total_demand"]) * 100,
    100)

beta_by_sku["Beta Service Level"] = beta_by_sku["Beta Service Level"].round(2)

final_df = final_df.merge(
    beta_by_sku[["SKU", "Beta Service Level"]],
    on="SKU",
    how="left")

optimization_summary_df = optimization_summary_df.merge(
    beta_by_sku,
    left_on="sku",
    right_on="SKU",
    how="left")

final_df["Date"] = pd.to_datetime(final_df["Date"], errors="coerce")
final_df = final_df.sort_values(["SKU", "Date"])
final_df["Date"] = final_df["Date"].dt.strftime("%d/%m/%Y")

# ========================= KPIS DASHBOARD POR SKU =========================

dashboard_kpis_df = (
    final_df.groupby("SKU", as_index=False)
    .agg(
        stock_cost=("Inventory Holding Cost", "sum"),
        total_demand=("Demand", "sum"),
        total_stockout=("Stockout", "sum"),
        stockout_days=("Stockout", lambda x: (x > 0).sum()),
        total_days=("Date", "count"),
        average_inventory_level=("SOH End", "mean"),
        avg_daily_demand=("Demand", "mean"),
        beta_service_level=("Beta Service Level", "mean")))

dashboard_kpis_df["Stockout Rate"] = np.where(
    dashboard_kpis_df["total_demand"] > 0,
    dashboard_kpis_df["total_stockout"] / dashboard_kpis_df["total_demand"] * 100,0)

dashboard_kpis_df["Alpha Service Level"] = np.where(
    dashboard_kpis_df["total_days"] > 0,
    (1 - dashboard_kpis_df["stockout_days"] / dashboard_kpis_df["total_days"]) * 100,100)

dashboard_kpis_df["Stock Coverage (days)"] = np.where(
    dashboard_kpis_df["avg_daily_demand"] > 0,
    dashboard_kpis_df["average_inventory_level"] / dashboard_kpis_df["avg_daily_demand"],0)

dashboard_kpis_df = dashboard_kpis_df.rename(columns={
    "stock_cost": "Stock Cost",
    "average_inventory_level": "Average Inventory Level",
    "beta_service_level": "Beta Service Level"})

dashboard_kpis_df = dashboard_kpis_df[
    [
        "SKU",
        "Stock Cost",
        "Stockout Rate",
        "Alpha Service Level",
        "Beta Service Level",
        "Average Inventory Level",
        "Stock Coverage (days)"]].round(2)

# ========================= EXPORTAR =========================

output_path = os.path.join(folder, output_file)
dashboard_kpis_output_path = output_path.replace(".csv", "_KPIs.csv")

final_df.to_csv(
    output_path,
    index=False,
    encoding="utf-8-sig",
    sep=";",
    decimal=",")

dashboard_kpis_df.to_csv(
    dashboard_kpis_output_path,
    index=False,
    encoding="utf-8-sig",
    sep=";",
    decimal=",")

# ========================= RESUMO =========================

print(f"\nCSV simulação guardado em: {output_path}")
print(f"\nCSV KPIs dashboard guardado em: {dashboard_kpis_output_path}")
print(f"\nSKUs simulados: {final_df['SKU'].nunique()}")
