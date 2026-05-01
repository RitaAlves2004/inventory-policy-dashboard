import os
import pandas as pd
import numpy as np

folder = r"C:\Users\madel\OneDrive\Ambiente de Trabalho\4 ano\GCA\Projeto 2026\parquet_filtered_final"

stock_file = "master_stock_forecast.parquet"
cost_file = "20260210_CustosProdutos.xlsx"
abc_file = "Demand_ABC.xlsx"
output_file = "AsIsMetrics.csv"

WACC = 0.02

def normalize_sku(s):
    return s.astype(str).str.strip().str.upper().str.replace(r"\s+", "", regex=True)

stock = pd.read_parquet(os.path.join(folder, stock_file))
costs = pd.read_excel(os.path.join(folder, cost_file))
abc = pd.read_excel(os.path.join(folder, abc_file))

stock["sku"] = normalize_sku(stock["sku"])
costs["sku"] = normalize_sku(costs["sku"])
abc["sku"] = normalize_sku(abc["sku"])

costs = (
    costs.assign(unit_cost=pd.to_numeric(costs["custo"], errors="coerce"))
    [["sku", "unit_cost"]]
    .dropna(subset=["sku"])
    .groupby("sku", as_index=False)
    .agg(unit_cost=("unit_cost", "mean"))
)

abc = (
    abc[["sku", "ABC_Class"]]
    .dropna(subset=["sku"])
    .drop_duplicates(subset=["sku"])
)

stock = stock.merge(costs, on="sku", how="left")
stock = stock.merge(abc, on="sku", how="left")

for col in ["stock_on_hand", "demand", "forecast", "unit_cost"]:
    stock[col] = pd.to_numeric(stock[col], errors="coerce").fillna(0)

stock["daily_holding_cost"] = stock["stock_on_hand"] * stock["unit_cost"] * (WACC / 365)
stock["stockout"] = np.maximum(stock["demand"] - stock["stock_on_hand"], 0)

sku_metrics = (
    stock.groupby("sku", as_index=False)
    .agg(
        ABC_Class=("ABC_Class", "first"),
        total_demand=("demand", "sum"),
        total_stockout=("stockout", "sum"),
        total_holding_cost=("daily_holding_cost", "sum"),
        average_inventory_level=("stock_on_hand", "mean"),
        average_daily_demand=("demand", "mean"),
        stockout_days=("stockout", lambda x: (x > 0).sum()),
        total_days=("sku", "count"),
        average_unit_cost=("unit_cost", "mean")
    )
)

sku_metrics["stock_out_rate_pct"] = np.where(
    sku_metrics["total_demand"] > 0,
    sku_metrics["total_stockout"] / sku_metrics["total_demand"] * 100,
    0
)

sku_metrics["alpha_service_level"] = np.where(
    sku_metrics["total_days"] > 0,
    (1 - sku_metrics["stockout_days"] / sku_metrics["total_days"]) * 100,
    100
)

sku_metrics["beta_service_level"] = np.where(
    sku_metrics["total_demand"] > 0,
    (1 - sku_metrics["total_stockout"] / sku_metrics["total_demand"]) * 100,
    100
)

sku_metrics["stock_coverage_days"] = np.where(
    sku_metrics["average_daily_demand"] > 0,
    sku_metrics["average_inventory_level"] / sku_metrics["average_daily_demand"],
    np.nan
)

round_cols = [
    "total_holding_cost",
    "stock_out_rate_pct",
    "alpha_service_level",
    "beta_service_level",
    "average_inventory_level",
    "average_daily_demand",
    "stock_coverage_days",
    "average_unit_cost"
]

sku_metrics[round_cols] = sku_metrics[round_cols].apply(
    lambda x: pd.to_numeric(x, errors="coerce").round(2)
)

sku_metrics = sku_metrics[[
    "sku",
    "ABC_Class",
    "average_unit_cost",
    "total_holding_cost",
    "total_demand",
    "total_stockout",
    "stock_out_rate_pct",
    "alpha_service_level",
    "beta_service_level",
    "average_inventory_level",
    "average_daily_demand",
    "stock_coverage_days",
    "stockout_days",
    "total_days"
]]

sku_metrics = sku_metrics.rename(columns={
    "sku": "SKU",
    "ABC_Class": "ABC Class",
    "average_unit_cost": "Average Unit Cost",
    "total_holding_cost": "Stock Cost",
    "total_demand": "Total Demand",
    "total_stockout": "Total Stockout",
    "stock_out_rate_pct": "Stockout Rate",
    "alpha_service_level": "Alpha Service Level",
    "beta_service_level": "Beta Service Level",
    "average_inventory_level": "Average Inventory Level",
    "average_daily_demand": "Average Daily Demand",
    "stock_coverage_days": "Stock Coverage (days)",
    "stockout_days": "Stockout Days",
    "total_days": "Total Days"
})

sku_metrics.to_csv(
    os.path.join(folder, output_file),
    index=False,
    sep=";",
    decimal=",",
    encoding="utf-8-sig"
)

print("CSV created successfully")
print(f"Output file: {output_file}")

print("\n===== SUMMARY =====")
print(f"Total SKUs: {sku_metrics['SKU'].nunique()}")
print(f"Total holding cost: {sku_metrics['Stock Cost'].sum():,.2f} €")
print(f"Average alpha service level: {sku_metrics['Alpha Service Level'].mean():,.2f}%")
print(f"Average beta service level: {sku_metrics['Beta Service Level'].mean():,.2f}%")