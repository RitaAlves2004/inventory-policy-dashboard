import os
import pandas as pd
import numpy as np
from statistics import NormalDist

# ========================= CONFIGURAÇÃO =========================

folder = "."

start_date = pd.Timestamp("2023-06-01")
service_level_candidates = np.round(np.arange(0.90, 0.991, 0.01), 2)
z_candidates = {sl: NormalDist().inv_cdf(sl) for sl in service_level_candidates}

transport_cost_per_unit = 0.15
WACC = 0.02

stock_file    = "master_stock_forecast.parquet"
review_file   = "20260210_review_days.parquet"
leadtime_file = "master_lead_times.parquet"
cost_file     = "20260210_CustosProdutos.xlsx"
policy_file   = "20260210_stock_policy_parameters.parquet"

output_file = "PolíticaSminSmáx.csv"

# ========================= FUNÇÕES AUXILIARES =========================

def normalize_sku(series):
    return series.astype(str).str.strip().str.upper().str.replace(r"\s+", "", regex=True)

def current_day_custom(date_series):
    return ((date_series.dt.weekday + 1) % 7) + 1

# ========================= LER FICHEIROS =========================

stock     = pd.read_parquet(os.path.join(folder, stock_file))
review    = pd.read_parquet(os.path.join(folder, review_file))
leadtimes = pd.read_parquet(os.path.join(folder, leadtime_file))
costs     = pd.read_excel(os.path.join(folder, cost_file))
policy    = pd.read_parquet(os.path.join(folder, policy_file))

for df in [stock, review, leadtimes, costs, policy]:
    df["sku"] = normalize_sku(df["sku"])

# ========================= LIMPEZA E PREPARAÇÃO =========================

stock["date"] = pd.to_datetime(stock["date"].astype(str), format="%Y%m%d", errors="coerce")

for col in ["forecast", "demand", "stock_on_hand"]:
    stock[col] = pd.to_numeric(stock[col], errors="coerce").fillna(0)

review["review_day"] = pd.to_numeric(review["review_day"], errors="coerce")

for col in ["avg_lead_time_real", "std_lead_time_real"]:
    leadtimes[col] = pd.to_numeric(
        leadtimes[col].astype(str).str.replace(",", ".", regex=False).str.strip(),
        errors="coerce")

costs["custo"] = pd.to_numeric(costs["custo"], errors="coerce").fillna(0)
policy["moq_units"] = pd.to_numeric(policy["moq_units"], errors="coerce").fillna(1).clip(lower=1)

stock = stock.loc[
    stock["date"].ge(start_date),
    ["sku", "date", "forecast", "demand", "stock_on_hand"]
].dropna(subset=["sku", "date"])

stock["forecast_error"] = stock["demand"] - stock["forecast"]

# ========================= LEAD TIMES =========================

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

policy["moq_units"] = policy["moq_units"].fillna(1).clip(lower=1)

# ========================= STOCK DIÁRIO BASE =========================

stock_daily = (
    stock.groupby(["sku", "date"], as_index=False)
    .agg(
        forecast=("forecast", "sum"),
        demand=("demand", "sum"),
        stock_on_hand=("stock_on_hand", "max"),
        forecast_error=("forecast_error", "sum"))
    .sort_values(["sku", "date"])
    .reset_index(drop=True))

sku_activity = stock_daily.groupby("sku", as_index=False).agg(
    total_forecast=("forecast", "sum"),
    total_demand=("demand", "sum"))

valid_skus = sku_activity.loc[
    (sku_activity["total_forecast"] != 0) | (sku_activity["total_demand"] != 0),
    "sku"]

stock_daily = stock_daily[stock_daily["sku"].isin(valid_skus)].copy()

forecast_std_df = stock_daily.groupby("sku", as_index=False).agg(
    std_forecast=("forecast_error", "std"))

forecast_std_df["std_forecast"] = forecast_std_df["std_forecast"].fillna(0)

initial_stock = (
    stock_daily.sort_values(["sku", "date"])
    .groupby("sku", as_index=False)
    .first()[["sku", "stock_on_hand"]]
    .rename(columns={"stock_on_hand": "initial_soh"}))

stock_daily = (
    stock_daily
    .merge(forecast_std_df, on="sku", how="left")
    .merge(initial_stock, on="sku", how="left")
    .merge(leadtimes, on="sku", how="inner")
    .merge(costs, on="sku", how="left")
    .merge(policy, on="sku", how="left"))

stock_daily = stock_daily.fillna({
    "std_forecast": 0,
    "initial_soh": 0,
    "unit_cost": 0,
    "transport_cost_per_unit": transport_cost_per_unit,
    "moq_units": 1})

stock_daily["moq_units"] = stock_daily["moq_units"].clip(lower=1)
stock_daily["total_unit_cost"] = stock_daily["unit_cost"] + stock_daily["transport_cost_per_unit"]
stock_daily["current_day"] = current_day_custom(stock_daily["date"])

# ========================= REVIEW DAYS =========================

review_clean = review.dropna(subset=["sku", "review_day"]).copy()
review_clean["review_day"] = review_clean["review_day"].astype(int)

review_days_map = (
    review_clean.groupby("sku")["review_day"]
    .apply(lambda x: set(x.astype(int).unique()))
    .to_dict())

all_skus = set(stock_daily["sku"].unique())

for sku in all_skus:
    if sku not in review_days_map or not review_days_map[sku]:
        review_days_map[sku] = {2}

review_days_str = pd.DataFrame({
    "sku": list(review_days_map.keys()),
    "review_days": [
        ",".join(map(str, sorted(days)))
        for days in review_days_map.values()]})

stock_daily = stock_daily.merge(review_days_str, on="sku", how="left")
stock_daily["review_days"] = stock_daily["review_days"].fillna("2")

stock_daily["is_review_day"] = [
    int(day) in review_days_map.get(sku, {2})
    for sku, day in zip(stock_daily["sku"], stock_daily["current_day"])]

# ========================= DIAS ATÉ PRÓXIMO REVIEW =========================

review_clean_full = pd.DataFrame([
    {"sku": sku, "review_day": day}
    for sku, days in review_days_map.items()
    for day in days])

review_expanded = stock_daily[["sku", "date", "current_day"]].merge(
    review_clean_full,
    on="sku",
    how="left")

review_expanded["days_until_review"] = (
    review_expanded["review_day"] - review_expanded["current_day"]) % 7

review_expanded["days_until_review"] = review_expanded["days_until_review"].replace(0, 7)

review_period_df = (
    review_expanded.groupby(["sku", "date"], as_index=False)
    .agg(review_period_days=("days_until_review", "min")))

stock_daily = stock_daily.merge(review_period_df, on=["sku", "date"], how="left")
stock_daily["review_period_days"] = stock_daily["review_period_days"].fillna(7).astype(int)

# ========================= FUNÇÕES DE SIMULAÇÃO =========================

def precompute_sku_arrays(sku_df):
    sku_df = sku_df.sort_values("date").reset_index(drop=True)

    return dict(
        dates=sku_df["date"].to_numpy(),
        forecasts=sku_df["forecast"].to_numpy(float),
        demands=sku_df["demand"].to_numpy(float),
        review_periods=sku_df["review_period_days"].to_numpy(int),
        avg_lead_times=sku_df["avg_lead_time_real"].to_numpy(float),
        std_lead_times=sku_df["std_lead_time_real"].to_numpy(float),
        std_forecasts=sku_df["std_forecast"].to_numpy(float),
        initial_soh=float(sku_df["initial_soh"].iloc[0]),
        unit_cost=sku_df["unit_cost"].to_numpy(float),
        transport_cost=sku_df["transport_cost_per_unit"].to_numpy(float),
        total_unit_cost=sku_df["total_unit_cost"].to_numpy(float),
        moq_units=sku_df["moq_units"].to_numpy(float),
        review_days=sku_df["review_days"].to_numpy(),
        is_review_day=sku_df["is_review_day"].to_numpy(bool),
        current_day=sku_df["current_day"].to_numpy(int),
        prefix_forecast=np.r_[0, np.cumsum(sku_df["forecast"].to_numpy(float))],
        n_rows=len(sku_df))

def compute_smin_smax(arrs, z):
    n = arrs["n_rows"]
    r_arr = np.maximum(1, arrs["review_periods"])
    lt_arr = arrs["avg_lead_times"]
    std_lt = np.maximum(0.0, arrs["std_lead_times"])
    sigma_f = np.maximum(0.0, arrs["std_forecasts"])
    prefix = arrs["prefix_forecast"]

    n_dynamic = np.maximum(1, 2 * r_arr)

    horizon_min = r_arr + lt_arr
    horizon_max = n_dynamic + lt_arr

    horizon_min_days = np.maximum(1, np.ceil(horizon_min).astype(int))
    horizon_max_days = np.maximum(1, np.ceil(horizon_max).astype(int))

    idx = np.arange(n)

    end_min = np.minimum(idx + 1 + horizon_min_days, n)
    end_max = np.minimum(idx + 1 + horizon_max_days, n)

    cs_min = prefix[end_min] - prefix[idx + 1]
    cs_max = prefix[end_max] - prefix[idx + 1]

    cs_min = np.where(idx + 1 >= n, 0.0, cs_min)
    cs_max = np.where(idx + 1 >= n, 0.0, cs_max)

    ratio = np.where(horizon_max > 0, horizon_min / horizon_max, 1.0)

    ss_min = z * sigma_f + z * std_lt * (cs_min * ratio)
    ss_max = z * sigma_f * np.sqrt(
        np.where(horizon_min > 0, horizon_max / horizon_min, 1.0)
    ) + z * std_lt * cs_max

    Smin = np.ceil(cs_min + ss_min).astype(int)
    Smax = np.ceil(cs_max + ss_max).astype(int)

    return cs_min, cs_max, ss_min, ss_max, Smin, Smax, n_dynamic.astype(int)

def simulate_policy(
    arrs,
    Smin,
    Smax,
    return_rows=False,
    sku=None,
    z=None,
    service_level=None,
    cs_min=None,
    cs_max=None,
    ss_min_arr=None,
    ss_max_arr=None,
    n_arr=None
):
    n = arrs["n_rows"]

    demands = arrs["demands"]
    forecasts = arrs["forecasts"]
    dates = arrs["dates"]

    is_review_day = arrs["is_review_day"]
    avg_lead_times = arrs["avg_lead_times"]
    std_lead_times = arrs["std_lead_times"]

    moq_units = arrs["moq_units"]
    unit_cost = arrs["unit_cost"]
    transport_cost = arrs["transport_cost"]

    review_days = arrs["review_days"]
    current_day = arrs["current_day"]
    review_periods = arrs["review_periods"]

    date0 = pd.Timestamp(dates[0])
    max_days = n + int(np.ceil(avg_lead_times.max())) + 5
    delivery_arr = np.zeros(max_days, dtype=float)

    soh_inicio = arrs["initial_soh"]
    pipeline_open = 0.0

    stockout_total = 0.0
    inventory_value_total = 0.0
    order_total_cost_total = 0.0

    rows = [] if return_rows else None

    for i in range(n):
        day_offset = int((pd.Timestamp(dates[i]) - date0).days)

        delivered = delivery_arr[day_offset] if day_offset < max_days else 0.0
        pipeline_open = max(0.0, pipeline_open - delivered)

        stock_position = soh_inicio + pipeline_open
        ordered = 0.0

        if is_review_day[i]:
            lt_days = max(1, int(np.ceil(avg_lead_times[i])))

            if stock_position <= Smin[i]:
                necessidade = max(0.0, Smax[i] - stock_position)

                if necessidade > 0:
                    ordered = max(necessidade, moq_units[i])
                    delivery_off = day_offset + lt_days

                    if delivery_off < max_days:
                        delivery_arr[delivery_off] += ordered

                    pipeline_open += ordered

        available_today = soh_inicio + delivered

        stockout = max(0.0, demands[i] - available_today)
        soh_final = max(0.0, available_today - demands[i])

        inventory_value = soh_final * unit_cost[i] * (WACC / 365)

        order_product_cost = ordered * unit_cost[i]
        order_transport_cost = ordered * transport_cost[i]
        order_total_cost = order_product_cost + order_transport_cost

        stockout_total += stockout
        inventory_value_total += inventory_value
        order_total_cost_total += order_total_cost

        if return_rows:
            rows.append({
                "SKU": sku,
                "Date": pd.Timestamp(dates[i]),
                "Forecast": int(np.round(forecasts[i])),
                "Demand": int(np.round(demands[i])),
                "Stock Position": int(np.round(stock_position)),
                "SOH Start": int(np.round(soh_inicio)),
                "Orders Placed": int(np.round(ordered)),
                "Orders Delivered": int(np.round(delivered)),
                "SOH End": int(np.round(soh_final)),
                "stockout": int(np.round(stockout)),
                "Cycle Stock min": int(np.ceil(cs_min[i])),
                "Cycle Stock max": int(np.ceil(cs_max[i])),
                "SS min": int(np.ceil(ss_min_arr[i])),
                "SS max": int(np.ceil(ss_max_arr[i])),
                "Smin": int(Smin[i]),
                "Smax": int(Smax[i]),
                "Inventory Holding Cost": round(inventory_value, 2),
                "Review Days": review_days[i],
                "Is Review Day": bool(is_review_day[i]),
                "Current Day": int(current_day[i]),
                "Review Period Days": int(review_periods[i]),
                "avg LT Real": round(avg_lead_times[i], 2),
                "std LT Real": round(std_lead_times[i], 2),
                "n": int(n_arr[i]),
                "MOQ": int(np.round(moq_units[i])),
                "Alpha Service Level": float(service_level),
                "z": float(z)})

        soh_inicio = soh_final

    metrics = {
        "service_level": service_level,
        "z": z,
        "stockout_total": stockout_total,
        "inventory_value_total": inventory_value_total,
        "order_total_cost_total": order_total_cost_total}

    return pd.DataFrame(rows) if return_rows else None, metrics

# ========================= OTIMIZAÇÃO POR SKU =========================

best_results = []
optimization_summary = []

print(f"A simular {stock_daily['sku'].nunique()} SKUs...")

for idx_sku, (sku, sku_base_df) in enumerate(stock_daily.groupby("sku", sort=True)):

    if idx_sku % 50 == 0:
        print(f"  SKU {idx_sku} / {stock_daily['sku'].nunique()}: {sku}")

    arrs = precompute_sku_arrays(sku_base_df)

    best_metrics = None
    best_sl_data = None

    for service_level in service_level_candidates:
        z = z_candidates[service_level]

        cs_min, cs_max, ss_min_a, ss_max_a, Smin, Smax, n_arr = compute_smin_smax(arrs, z)

        _, metrics = simulate_policy(
            arrs,
            Smin,
            Smax,
            return_rows=False,
            service_level=service_level,
            z=z)

        if best_metrics is None or (
            metrics["stockout_total"] < best_metrics["stockout_total"] or (
                metrics["stockout_total"] == best_metrics["stockout_total"] and
                metrics["inventory_value_total"] < best_metrics["inventory_value_total"]
            ) or (
                metrics["stockout_total"] == best_metrics["stockout_total"] and
                metrics["inventory_value_total"] == best_metrics["inventory_value_total"] and
                metrics["order_total_cost_total"] < best_metrics["order_total_cost_total"]
            )
        ):
            best_metrics = metrics
            best_sl_data = (cs_min, cs_max, ss_min_a, ss_max_a, Smin, Smax, n_arr)

    cs_min, cs_max, ss_min_a, ss_max_a, Smin, Smax, n_arr = best_sl_data

    best_sim, _ = simulate_policy(
        arrs,
        Smin,
        Smax,
        return_rows=True,
        sku=sku,
        z=best_metrics["z"],
        service_level=best_metrics["service_level"],
        cs_min=cs_min,
        cs_max=cs_max,
        ss_min_arr=ss_min_a,
        ss_max_arr=ss_max_a,
        n_arr=n_arr)

    best_results.append(best_sim)

    optimization_summary.append({
        "sku": sku,
        "optimal_service_level": best_metrics["service_level"],
        "optimal_z": best_metrics["z"],
        "stockout_total": best_metrics["stockout_total"],
        "inventory_value_total": best_metrics["inventory_value_total"],
        "order_total_cost_total": best_metrics["order_total_cost_total"]})

print("Simulação concluída. A construir dataframe final...")

# ========================= DATAFRAMES FINAIS =========================

final_df = pd.concat(best_results, ignore_index=True)
optimization_summary_df = pd.DataFrame(optimization_summary)

# ========================= BETA SERVICE LEVEL POR SKU =========================

beta_by_sku = (
    final_df.groupby("SKU", as_index=False)
    .agg(
        total_demand=("Demand", "sum"),
        total_stockout=("stockout", "sum")))

beta_by_sku["beta_service_level"] = np.where(
    beta_by_sku["total_demand"] > 0,
    (1 - beta_by_sku["total_stockout"] / beta_by_sku["total_demand"]) * 100,
    100)

beta_by_sku["beta_service_level"] = beta_by_sku["beta_service_level"].round(2)

final_df = final_df.merge(
    beta_by_sku[["SKU", "beta_service_level"]],
    on="SKU",
    how="left")

optimization_summary_df = optimization_summary_df.merge(
    beta_by_sku,
    left_on="sku",
    right_on="SKU",
    how="left")

# ========================= KPIs POR SKU =========================

kpis_sku = (
    final_df.groupby("SKU", as_index=False)
    .agg(
        custo_stock_total=("Inventory Holding Cost", "sum"),
        total_demand=("Demand", "sum"),
        total_stockout=("stockout", "sum"),
        dias_com_stockout=("stockout", lambda x: (x > 0).sum()),
        dias_total=("Date", "count"),
        average_inventory_level_quantidade=("SOH End", "mean"),
        avg_daily_demand=("Demand", "mean"),
        service_level_otimo=("Alpha Service Level", "first"),
        z_otimo=("z", "first")))

kpis_sku["stock_out_rate_%"] = np.where(
    kpis_sku["total_demand"] > 0,
    (kpis_sku["total_stockout"] / kpis_sku["total_demand"]) * 100,
    0)

kpis_sku["alpha_service_level_%"] = np.where(
    kpis_sku["dias_total"] > 0,
    (1 - kpis_sku["dias_com_stockout"] / kpis_sku["dias_total"]) * 100,
    100)

kpis_sku["beta_service_level_%"] = np.where(
    kpis_sku["total_demand"] > 0,
    (1 - kpis_sku["total_stockout"] / kpis_sku["total_demand"]) * 100,
    100)

kpis_sku["stock_coverage_dias"] = np.where(
    kpis_sku["avg_daily_demand"] > 0,
    kpis_sku["average_inventory_level_quantidade"] / kpis_sku["avg_daily_demand"],
    np.nan)

cols_round = [
    "custo_stock_total",
    "stock_out_rate_%",
    "alpha_service_level_%",
    "beta_service_level_%",
    "average_inventory_level_quantidade",
    "avg_daily_demand",
    "stock_coverage_dias",
    "service_level_otimo",
    "z_otimo"]

for col in cols_round:
    kpis_sku[col] = pd.to_numeric(kpis_sku[col], errors="coerce").round(2)

kpis_sku = kpis_sku.rename(columns={
    "custo_stock_total": "Stock Cost",
    "stock_out_rate_%": "Stockout Rate",
    "alpha_service_level_%": "Alpha Service Level",
    "beta_service_level_%": "Beta Service Level",
    "average_inventory_level_quantidade": "Average Inventory Level",
    "stock_coverage_dias": "Stock Coverage (days)"})

kpis_sku = kpis_sku[[
    "SKU",
    "Stock Cost",
    "Stockout Rate",
    "Alpha Service Level",
    "Beta Service Level",
    "Average Inventory Level",
    "Stock Coverage (days)"]]

# ========================= ORDEM DAS COLUNAS FINAL_DF =========================

final_df = final_df[[
    "SKU","Date",
    "Forecast","Demand","Stock Position","SOH Start","Orders Placed","Orders Delivered",
    "SOH End","stockout","Cycle Stock min",
    "Cycle Stock max","SS min","SS max","Smin","Smax","Inventory Holding Cost",
    "Review Days","Is Review Day",
    "Current Day","Review Period Days","avg LT Real",
    "std LT Real","n","MOQ","Alpha Service Level","z"]].sort_values(["SKU", "Date"])

final_df = final_df.rename(columns={
    "stockout": "Stockout"})

# ========================= FORMATAÇÃO =========================

decimal_cols = [
    "Inventory Holding Cost",
    "avg LT Real",
    "std LT Real",
    "Alpha Service Level"]

for col in decimal_cols:
    final_df[col] = pd.to_numeric(final_df[col], errors="coerce").round(2)

final_df["z"] = pd.to_numeric(final_df["z"], errors="coerce").round(4)

final_df["Date"] = pd.to_datetime(final_df["Date"], errors="coerce")
final_df = final_df.sort_values(["SKU", "Date"])
final_df["Date"] = final_df["Date"].dt.strftime("%d/%m/%Y")

# ========================= EXPORTAR CSV =========================

output_path = os.path.join(folder, output_file)
kpis_output_path = output_path.replace(".csv", "_KPIs.csv")

final_df.to_csv(
    output_path,
    index=False,
    encoding="utf-8-sig",
    sep=";",
    decimal=",")

kpis_sku.to_csv(
    kpis_output_path,
    index=False,
    encoding="utf-8-sig",
    sep=";",
    decimal=",")

# ========================= RESUMO =========================

print(f"\nCSV simulação guardado em: {output_path}")
print(f"\nCSV KPIs por SKU guardado em: {kpis_output_path}")
print(f"\nSKUs simulados: {final_df['SKU'].nunique()}")
