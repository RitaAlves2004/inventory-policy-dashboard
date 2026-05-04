import os
import pandas as pd
import numpy as np

folder = r"C:\Users\madel\OneDrive\Ambiente de Trabalho\4 ano\GCA\Projeto 2026\parquet_filtered_final"
xyz_file = r"C:\Users\madel\OneDrive\Ambiente de Trabalho\4 ano\GCA\Projeto 2026\AS_IS_G5\Costumer analysis\results\xyz_stock_policy\xyz_stock_coverage_details.xlsx"

stock_file = "master_stock_forecast.parquet"
cost_file = "20260210_CustosProdutos.xlsx"
abc_file = "Demand_ABC.xlsx"
output_file = "AsIsMetrics.csv"

order_files = [
    f"20260210_store_orders_{m}_2023.parquet"
    for m in ["jun", "jul", "aug", "sep", "oct", "nov"]]

WACC, transport_cost_per_unit = 0.02, 0.15
start_date = pd.Timestamp("2023-06-01")


def normalize_sku(s):
    return s.astype(str).str.strip().str.upper().str.replace(r"\s+", "", regex=True)

stock = pd.read_parquet(os.path.join(folder, stock_file))
costs = pd.read_excel(os.path.join(folder, cost_file))
abc = pd.read_excel(os.path.join(folder, abc_file))
xyz = pd.read_excel(xyz_file)
orders = pd.concat([pd.read_parquet(os.path.join(folder, f)) for f in order_files], ignore_index=True)

for df in [stock, costs, abc, xyz, orders]:
    df["sku"] = normalize_sku(df["sku"])

costs = (
    costs.assign(unit_cost=pd.to_numeric(costs["custo"], errors="coerce"))
    [["sku", "unit_cost"]]
    .dropna(subset=["sku"])
    .groupby("sku", as_index=False)
    .agg(unit_cost=("unit_cost", "mean")))

abc = abc[["sku", "ABC_Class"]].dropna(subset=["sku"]).drop_duplicates("sku")

xyz = (
    xyz[["sku", "xyz"]]
    .dropna(subset=["sku"])
    .drop_duplicates("sku")
    .rename(columns={"xyz": "XYZ_Class"}))

orders["deliver_date"] = pd.to_datetime(orders["deliver_date"], errors="coerce")
orders["delivered_units"] = pd.to_numeric(orders["delivered_units"], errors="coerce").fillna(0)

orders = (
    orders.loc[orders["deliver_date"].ge(start_date)]
    .dropna(subset=["sku", "deliver_date"])
    .merge(costs, on="sku", how="left"))

orders["unit_cost"] = orders["unit_cost"].fillna(0)
orders["order_total_cost"] = orders["delivered_units"] * (orders["unit_cost"] + transport_cost_per_unit)

order_costs = (
    orders.groupby("sku", as_index=False)
    .agg(
        total_order_cost=("order_total_cost", "sum"),
        total_ordered_units=("delivered_units", "sum")))

stock = (
    stock.merge(costs, on="sku", how="left")
    .merge(abc, on="sku", how="left")
    .merge(xyz, on="sku", how="left")
    .merge(order_costs, on="sku", how="left"))

stock[["total_order_cost", "total_ordered_units"]] = stock[["total_order_cost", "total_ordered_units"]].fillna(0)

stock["date"] = pd.to_datetime(stock["date"].astype(str), format="%Y%m%d", errors="coerce")

for col in ["stock_on_hand", "demand", "forecast", "unit_cost"]:
    stock[col] = pd.to_numeric(stock[col], errors="coerce").fillna(0)

stock = stock.loc[stock["date"].ge(start_date)].dropna(subset=["sku", "date"])

sku_activity = stock.groupby("sku", as_index=False).agg(
    total_forecast=("forecast", "sum"),
    total_demand=("demand", "sum"))

valid_skus = sku_activity.loc[
    (sku_activity["total_forecast"] != 0) | (sku_activity["total_demand"] != 0),
    "sku"]

stock = stock[stock["sku"].isin(valid_skus)].copy()

stock["stockout"] = np.maximum(stock["demand"] - stock["stock_on_hand"], 0)
stock["soh_end"] = np.maximum(stock["stock_on_hand"] - stock["demand"], 0)
stock["daily_holding_cost"] = stock["soh_end"] * stock["unit_cost"] * (WACC / 365)

sku_metrics = (
    stock.groupby("sku", as_index=False)
    .agg(
        ABC_Class=("ABC_Class", "first"),
        XYZ_Class=("XYZ_Class", "first"),
        total_demand=("demand", "sum"),
        total_stockout=("stockout", "sum"),
        total_holding_cost=("daily_holding_cost", "sum"),
        total_order_cost=("total_order_cost", "first"),
        total_ordered_units=("total_ordered_units", "first"),
        average_inventory_level=("stock_on_hand", "mean"),
        average_daily_demand=("demand", "mean"),
        stockout_days=("stockout", lambda x: (x > 0).sum()),
        total_days=("sku", "count"),
        average_unit_cost=("unit_cost", "mean")))

sku_metrics["total_cost"] = sku_metrics["total_holding_cost"] + sku_metrics["total_order_cost"]

sku_metrics["stock_out_rate_pct"] = np.where(
    sku_metrics["total_demand"] > 0,
    sku_metrics["total_stockout"] / sku_metrics["total_demand"] * 100,0)

sku_metrics["alpha_service_level"] = np.where(
    sku_metrics["total_days"] > 0,
    (1 - sku_metrics["stockout_days"] / sku_metrics["total_days"]) * 100,
    100)

sku_metrics["beta_service_level"] = np.where(
    sku_metrics["total_demand"] > 0,
    (1 - sku_metrics["total_stockout"] / sku_metrics["total_demand"]) * 100,
    100)

sku_metrics["stock_coverage_days"] = np.where(
    sku_metrics["average_daily_demand"] > 0,
    sku_metrics["average_inventory_level"] / sku_metrics["average_daily_demand"],
    np.nan)

round_cols = [
    "total_holding_cost", "total_order_cost", "total_cost", "total_ordered_units",
    "stock_out_rate_pct", "alpha_service_level", "beta_service_level",
    "average_inventory_level", "average_daily_demand", "stock_coverage_days",
    "average_unit_cost"]

sku_metrics[round_cols] = sku_metrics[round_cols].apply(pd.to_numeric, errors="coerce").round(2)

sku_metrics = sku_metrics[[
    "sku", "ABC_Class", "XYZ_Class", "average_unit_cost",
    "total_holding_cost", "total_order_cost", "total_cost", "total_ordered_units",
    "total_demand", "total_stockout", "stock_out_rate_pct",
    "alpha_service_level", "beta_service_level", "average_inventory_level",
    "average_daily_demand", "stock_coverage_days", "stockout_days", "total_days"
]].rename(columns={
    "sku": "SKU",
    "ABC_Class": "ABC Class",
    "XYZ_Class": "XYZ Class",
    "average_unit_cost": "Average Unit Cost",
    "total_holding_cost": "Stock Cost",
    "total_order_cost": "Order Cost",
    "total_cost": "Total Cost",
    "total_ordered_units": "Total Ordered Units",
    "total_demand": "Total Demand",
    "total_stockout": "Total Stockout",
    "stock_out_rate_pct": "Stockout Rate",
    "alpha_service_level": "Alpha Service Level",
    "beta_service_level": "Beta Service Level",
    "average_inventory_level": "Average Inventory Level",
    "average_daily_demand": "Average Daily Demand",
    "stock_coverage_days": "Stock Coverage (days)",
    "stockout_days": "Stockout Days",
    "total_days": "Total Days"})

sku_metrics.to_csv(
    os.path.join(folder, output_file),
    index=False,
    sep=";",
    decimal=",",
    encoding="utf-8-sig")

print("CSV created successfully")
print(f"Output file: {output_file}")

print("\n===== SUMMARY =====")
print(f"Total SKUs: {sku_metrics['SKU'].nunique()}")
print(f"Total stock cost: {sku_metrics['Stock Cost'].sum():,.2f} €")
print(f"Total order cost: {sku_metrics['Order Cost'].sum():,.2f} €")
print(f"Total cost: {sku_metrics['Total Cost'].sum():,.2f} €")
print(f"Average alpha service level: {sku_metrics['Alpha Service Level'].mean():,.2f}%")
print(f"Average beta service level: {sku_metrics['Beta Service Level'].mean():,.2f}%")