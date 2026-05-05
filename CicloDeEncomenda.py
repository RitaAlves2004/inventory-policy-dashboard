import os
import pandas as pd
import numpy as np
from collections import defaultdict
from statistics import NormalDist

# ========================= CONFIGURAÇÃO =========================

folder = "."

start_date = pd.Timestamp("2023-06-01")
service_level_candidates = np.round(np.arange(0.60, 0.991, 0.01), 2)
z_candidates = {sl: NormalDist().inv_cdf(sl) for sl in service_level_candidates}

transport_cost_per_unit = 0.15
WACC = 0.02

files = {
    "stock": "master_stock_forecast.parquet",
    "review": "20260210_review_days.parquet",
    "leadtimes": "master_lead_times.parquet",
    "costs": "20260210_CustosProdutos.xlsx",
    "policy": "20260210_stock_policy_parameters.parquet"}

output_file = "PolíticaCicloDeEncomenda.csv"

# ========================= FUNÇÕES =========================

def normalize_sku(s):
    return s.astype(str).str.strip().str.upper().str.replace(r"\s+", "", regex=True)

def current_day_custom(d):
    return ((d.dt.weekday + 1) % 7) + 1

def make_forward_sum(values):
    return np.r_[0, np.cumsum(np.asarray(values, dtype=float))]

def sum_forward_prefix(prefix, start_idx, window):
    if window <= 0 or start_idx >= len(prefix) - 1:
        return 0.0
    end_idx = min(start_idx + window, len(prefix) - 1)
    return float(prefix[end_idx] - prefix[start_idx])

def days_until_next_review(current_day, review_days):
    if not review_days:
        return 7

    review_days = sorted(review_days)
    future_days = [d for d in review_days if d > current_day]

    if future_days:
        return future_days[0] - current_day

    return 7 - current_day + review_days[0]

# ========================= LER FICHEIROS =========================

stock = pd.read_parquet(os.path.join(folder, files["stock"]))
review = pd.read_parquet(os.path.join(folder, files["review"]))
leadtimes = pd.read_parquet(os.path.join(folder, files["leadtimes"]))
costs = pd.read_excel(os.path.join(folder, files["costs"]))
policy = pd.read_parquet(os.path.join(folder, files["policy"]))

for df in [stock, review, leadtimes, costs, policy]:
    df["sku"] = normalize_sku(df["sku"])

# ========================= LIMPEZA =========================

stock["date"] = pd.to_datetime(stock["date"].astype(str), format="%Y%m%d", errors="coerce")

for col in ["forecast", "demand", "stock_on_hand"]:
    stock[col] = pd.to_numeric(stock[col], errors="coerce").fillna(0)

review["review_day"] = pd.to_numeric(review["review_day"], errors="coerce")

for col in ["avg_lead_time_real", "std_lead_time_real"]:
    leadtimes[col] = pd.to_numeric(
        leadtimes[col].astype(str).str.replace(",", ".", regex=False).str.strip(),
        errors="coerce")

costs["custo"] = pd.to_numeric(costs["custo"], errors="coerce").fillna(0)
policy["moq_units"] = pd.to_numeric(policy["moq_units"], errors="coerce").fillna(1)

# ========================= STOCK =========================

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

# ========================= LEAD TIMES =========================

leadtimes = (
    leadtimes[["sku", "avg_lead_time_real", "std_lead_time_real"]]
    .dropna(subset=["sku", "avg_lead_time_real"])
    .groupby("sku", as_index=False)
    .mean())

leadtimes["std_lead_time_real"] = leadtimes["std_lead_time_real"].fillna(0)
leadtimes = leadtimes[leadtimes["avg_lead_time_real"] > 0]

# ========================= CUSTOS E MOQ =========================

costs = (
    costs[["sku", "custo"]]
    .dropna(subset=["sku"])
    .groupby("sku", as_index=False)
    .agg(unit_cost=("custo", "mean")))

costs["transport_cost_per_unit"] = transport_cost_per_unit
costs["total_unit_cost"] = costs["unit_cost"] + costs["transport_cost_per_unit"]

policy = (
    policy[["sku", "moq_units"]]
    .dropna(subset=["sku"])
    .groupby("sku", as_index=False)
    .agg(moq_units=("moq_units", "max")))

# ========================= REVIEW DAYS =========================

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

review_days_str = (
    pd.DataFrame({
        "sku": list(review_days_map.keys()),
        "review_days": [
            ",".join(map(str, sorted(days)))
            for days in review_days_map.values()]}))

# ========================= STOCK DIÁRIO BASE =========================

stock_daily = (
    stock.groupby(["sku", "date"], as_index=False)
    .agg(
        forecast=("forecast", "sum"),
        demand=("demand", "sum"),
        stock_on_hand=("stock_on_hand", "max"),
        forecast_error=("forecast_error", "sum"))
    .sort_values(["sku", "date"]))

std_forecast = stock_daily.groupby("sku", as_index=False).agg(
    std_forecast=("forecast_error", "std"))

initial_stock = (
    stock_daily.sort_values(["sku", "date"])
    .groupby("sku", as_index=False)
    .first()[["sku", "stock_on_hand"]]
    .rename(columns={"stock_on_hand": "initial_soh"}))

stock_daily = (
    stock_daily
    .merge(std_forecast, on="sku", how="left")
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

stock_daily["total_unit_cost"] = stock_daily["unit_cost"] + stock_daily["transport_cost_per_unit"]
stock_daily["moq_units"] = stock_daily["moq_units"].clip(lower=1)
stock_daily["current_day"] = current_day_custom(stock_daily["date"])

review_sets = stock_daily["sku"].map(review_days_map)

stock_daily["is_review_day"] = [
    int(day) in review_set if isinstance(review_set, set) else False
    for day, review_set in zip(stock_daily["current_day"], review_sets)]

stock_daily["review_cycle_days"] = [
    days_until_next_review(int(day), review_set if isinstance(review_set, set) else set())
    for day, review_set in zip(stock_daily["current_day"], review_sets)]

# ========================= SIMULAÇÃO RÁPIDA POR SKU =========================

def simulate_sku_fast(sku, sku_df, service_level, return_rows=False):
    sku_df = sku_df.sort_values("date").reset_index(drop=True)

    z = z_candidates[service_level]

    dates = sku_df["date"].to_numpy()
    forecasts = sku_df["forecast"].to_numpy(float)
    demands = sku_df["demand"].to_numpy(float)
    avg_lt = sku_df["avg_lead_time_real"].to_numpy(float)
    std_lt = sku_df["std_lead_time_real"].to_numpy(float)
    sigma_daily_arr = sku_df["std_forecast"].to_numpy(float)
    review_cycle_arr = sku_df["review_cycle_days"].to_numpy(float)

    unit_cost = sku_df["unit_cost"].to_numpy(float)
    total_unit_cost = sku_df["total_unit_cost"].to_numpy(float)
    moq_units = sku_df["moq_units"].to_numpy(float)
    is_review_day = sku_df["is_review_day"].to_numpy(bool)
    review_days = sku_df["review_days"].to_numpy()
    current_day = sku_df["current_day"].to_numpy(int)

    initial_soh = float(sku_df.loc[0, "initial_soh"])
    prefix_forecast = make_forward_sum(forecasts)

    avg_daily_demand = max(0.0, float(np.mean(forecasts)))
    if avg_daily_demand <= 0:
        avg_daily_demand = max(0.0, float(np.mean(demands)))

    n = len(sku_df)

    protection_period_days = np.zeros(n)
    cycle_demand_mean = np.zeros(n)
    cycle_demand_sigma = np.zeros(n)
    safety_stock_SS = np.zeros(n)
    order_cycle_level_S = np.zeros(n)

    for i in range(n):
        lead_time = max(0.0, avg_lt[i])
        std_lead_time = max(0.0, std_lt[i])
        sigma_daily = max(0.0, sigma_daily_arr[i])
        review_cycle = max(1.0, review_cycle_arr[i])

        protection_period = lead_time + review_cycle
        protection_days = max(1, int(np.ceil(protection_period)))

        mean = sum_forward_prefix(prefix_forecast, i, protection_days)
        if mean <= 0:
            mean = avg_daily_demand * protection_period

        sigma = np.sqrt(max(
            0.0,
            protection_period * sigma_daily**2 + avg_daily_demand**2 * std_lead_time**2))

        ss = z * sigma
        level = mean + ss

        protection_period_days[i] = int(np.ceil(protection_period))
        cycle_demand_mean[i] = int(np.ceil(mean))
        cycle_demand_sigma[i] = round(sigma, 2)
        safety_stock_SS[i] = int(np.ceil(ss))
        order_cycle_level_S[i] = int(np.ceil(level))

    scheduled_deliveries = defaultdict(float)
    soh_inicio = initial_soh
    pipeline_open = 0.0

    rows = []

    stockout_total = 0.0
    inventory_value_total = 0.0
    order_total_cost_total = 0.0

    for i in range(n):
        date = pd.Timestamp(dates[i])

        delivered = float(scheduled_deliveries.get(date, 0.0))
        pipeline_open -= delivered

        stock_position = soh_inicio + pipeline_open
        ordered = 0.0
        order_triggered = False

        if is_review_day[i]:
            need = order_cycle_level_S[i] - stock_position

            if need > 0:
                sampled_lead_time = np.random.normal(avg_lt[i], std_lt[i])
                lead_days = max(1, int(np.round(sampled_lead_time)))

                ordered = float(np.ceil(max(need, moq_units[i])))
                order_triggered = True

                delivery_date = date + pd.Timedelta(days=lead_days)
                scheduled_deliveries[delivery_date] += ordered
                pipeline_open += ordered

        available = soh_inicio + delivered
        stockout = max(0.0, demands[i] - available)
        soh_final = max(0.0, available - demands[i])

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
                "Review Cycle Days": int(review_cycle_arr[i]),
                "Review Days": review_days[i],
                "Current Day": int(current_day[i]),
                "Protection Period Days": int(protection_period_days[i]),
                "MOQ": int(np.ceil(moq_units[i])),
                "Order Up To Level S": int(order_cycle_level_S[i]),
                "SS": int(safety_stock_SS[i]),
                "avg LT Real": round(avg_lt[i], 4),
                "std LT Real": round(std_lt[i], 4),
                "Demand Mean": int(cycle_demand_mean[i]),
                "Demand Sigma": round(float(cycle_demand_sigma[i]), 2),
                "Inventory Holding Cost": round(inventory_holding_cost_day, 6),
                "Order Total Cost": round(order_total_cost, 6),
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

# ========================= OTIMIZAÇÃO POR SKU =========================

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
            best_metrics["order_total_cost_total"]
        ):
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

# ========================= DATAFRAMES FINAIS =========================

final_df = pd.concat(best_results, ignore_index=True).sort_values(["SKU", "Date"])
optimization_summary_df = pd.DataFrame(optimization_summary)

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

# ========================= KPIS DASHBOARD POR SKU =========================

dashboard_kpis_df = (
    final_df.groupby("SKU", as_index=False)
    .agg(
        stock_cost=("Inventory Holding Cost", "sum"),
        order_total_cost=("Order Total Cost", "sum"),
        total_demand=("Demand", "sum"),
        total_stockout=("Stockout", "sum"),
        stockout_days=("Stockout", lambda x: (x > 0).sum()),
        total_days=("Date", "count"),
        average_inventory_level=("SOH End", "mean"),
        avg_daily_demand=("Demand", "mean"),
        beta_service_level=("Beta Service Level", "mean")))

dashboard_kpis_df["total_cost"] = (
    dashboard_kpis_df["stock_cost"] + dashboard_kpis_df["order_total_cost"]
)

dashboard_kpis_df["stock_out_rate"] = np.where(
    dashboard_kpis_df["total_demand"] > 0,
    dashboard_kpis_df["total_stockout"] / dashboard_kpis_df["total_demand"] * 100,0)

dashboard_kpis_df["alpha_service_level"] = np.where(
    dashboard_kpis_df["total_days"] > 0,
    (1 - dashboard_kpis_df["stockout_days"] / dashboard_kpis_df["total_days"]) * 100,100)

dashboard_kpis_df["stock_coverage_days"] = np.where(
    dashboard_kpis_df["avg_daily_demand"] > 0,
    dashboard_kpis_df["average_inventory_level"] / dashboard_kpis_df["avg_daily_demand"],0)

dashboard_kpis_df = dashboard_kpis_df[
    [
        "SKU","total_cost","stock_out_rate","alpha_service_level",
        "beta_service_level","average_inventory_level","stock_coverage_days"]].round(2)

dashboard_kpis_df = dashboard_kpis_df.rename(columns={
    "total_cost": "Total Cost",
    "stock_out_rate": "Stockout Rate",
    "alpha_service_level": "Alpha Service Level",
    "beta_service_level": "Beta Service Level",
    "average_inventory_level": "Average Inventory Level",
    "stock_coverage_days": "Stock Coverage (days)"})

# ========================= FORMATAR DATA =========================

final_df["Date"] = pd.to_datetime(final_df["Date"]).dt.strftime("%d/%m/%Y")

# ========================= EXPORTAÇÃO CSV =========================

output_path = os.path.join(folder, output_file)

dashboard_kpis_output_path = output_path.replace(
    ".csv",
    "_KPIs.csv")

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

# ========================= OUTPUT FINAL =========================

print(f"\nCSV simulação guardado em:\n{output_path}")
print(f"\nCSV KPIs dashboard guardado em:\n{dashboard_kpis_output_path}")
print(f"\nSKUs simulados: {final_df['SKU'].nunique()}")
