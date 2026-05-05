import os
import pandas as pd
import numpy as np
from collections import defaultdict

# ========================= CONFIG =========================

folder = "."

N_SIMULATIONS = 100
WACC = 0.02
start_date = pd.Timestamp("2023-06-01")

POLICIES = {
    "Smin-Smax Policy": "PolíticaSminSmáx.csv",
    "Reorder Level Policy": "PolíticaNívelDeEncomenda.csv",
    "Order Cycle Policy": "PolíticaCicloDeEncomenda.csv",
}

output_file = "MonteCarlo_LeadTime_AllPolicies.csv"

# ========================= FUNÇÕES =========================

def load_policy_csv(path):
    return pd.read_csv(
        path,
        sep=";",
        decimal=",",
        encoding="utf-8-sig"
    )


def normalize_policy_df(df):
    rename_map = {
        "SKU": "sku",
        "Date": "date",
        "Demand": "demand",
        "SOH Start": "soh_start",
        "SOH End": "soh_end",
        "Orders Placed": "orders_placed",
        "Stockout": "stockout",
        "avg LT Real": "avg_lt",
        "std LT Real": "std_lt",
        "Inventory Holding Cost": "inventory_holding_cost",
        "Order Total Cost": "order_total_cost",
    }

    df = df.rename(columns=rename_map)

    df["sku"] = df["sku"].astype(str).str.strip().str.upper()
    df["date"] = pd.to_datetime(df["date"], dayfirst=True, errors="coerce")

    numeric_cols = [
        "demand",
        "soh_start",
        "soh_end",
        "orders_placed",
        "stockout",
        "avg_lt",
        "std_lt",
        "inventory_holding_cost",
        "order_total_cost",
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    df = df.dropna(subset=["sku", "date"])
    df = df[df["date"] >= start_date]

    return df


def infer_unit_cost(df):
    df = df.copy()

    df["unit_cost_inferred"] = np.where(
        df["soh_end"] > 0,
        df["inventory_holding_cost"] / df["soh_end"] / (WACC / 365),
        np.nan
    )

    sku_unit_cost = (
        df.groupby("sku")["unit_cost_inferred"]
        .median()
        .reset_index()
        .rename(columns={"unit_cost_inferred": "unit_cost"})
    )

    df = df.merge(sku_unit_cost, on="sku", how="left")
    df["unit_cost"] = df["unit_cost"].fillna(0)

    return df


def simulate_policy_once(policy_df, seed):
    rng = np.random.default_rng(seed)

    all_rows = []

    for sku, sku_df in policy_df.groupby("sku", sort=True):
        sku_df = sku_df.sort_values("date").reset_index(drop=True)

        scheduled_deliveries = defaultdict(float)

        soh_inicio = float(sku_df.loc[0, "soh_start"])

        rows = []

        for i in range(len(sku_df)):
            date = pd.Timestamp(sku_df.loc[i, "date"])

            demand = float(sku_df.loc[i, "demand"])
            ordered = float(sku_df.loc[i, "orders_placed"])
            avg_lt = max(0.0, float(sku_df.loc[i, "avg_lt"]))
            std_lt = max(0.0, float(sku_df.loc[i, "std_lt"]))
            unit_cost = float(sku_df.loc[i, "unit_cost"])
            order_total_cost_original = float(sku_df.loc[i, "order_total_cost"])

            delivered = float(scheduled_deliveries.get(date, 0.0))

            available = soh_inicio + delivered
            stockout = max(0.0, demand - available)
            soh_final = max(0.0, available - demand)

            inventory_holding_cost = soh_final * unit_cost * (WACC / 365)

            if ordered > 0:
                if avg_lt > 0 and std_lt > 0:
                    sigma_lognormal = np.sqrt(np.log(1 + (std_lt / avg_lt) ** 2))
                    mu_lognormal = np.log(avg_lt) - 0.5 * sigma_lognormal ** 2

                    sampled_lt = rng.lognormal(
                        mean=mu_lognormal,
                        sigma=sigma_lognormal
                    )
                else:
                    sampled_lt = avg_lt

                lead_days = max(1, int(np.ceil(sampled_lt)))

                delivery_date = date + pd.Timedelta(days=lead_days)
                scheduled_deliveries[delivery_date] += ordered

            rows.append({
                "sku": sku,
                "date": date,
                "demand": demand,
                "soh_start": soh_inicio,
                "orders_placed": ordered,
                "orders_delivered": delivered,
                "soh_end": soh_final,
                "stockout": stockout,
                "inventory_holding_cost": inventory_holding_cost,
                "order_total_cost": order_total_cost_original,
            })

            soh_inicio = soh_final

        all_rows.append(pd.DataFrame(rows))

    return pd.concat(all_rows, ignore_index=True)


def calculate_mc_kpis(sim_df):
    total_demand = sim_df["demand"].sum()
    total_stockout = sim_df["stockout"].sum()
    total_soh = sim_df["soh_end"].sum()
    total_stock_cost = sim_df["inventory_holding_cost"].sum()
    total_order_cost = sim_df["order_total_cost"].sum()
    total_cost = total_stock_cost + total_order_cost

    total_days = len(sim_df)
    stockout_days = (sim_df["stockout"] > 0).sum()

    beta_service_level = (
        (1 - total_stockout / total_demand) * 100
        if total_demand > 0 else 100
    )

    stockout_rate = (
        total_stockout / total_demand * 100
        if total_demand > 0 else 0
    )

    alpha_service_level = (
        (1 - stockout_days / total_days) * 100
        if total_days > 0 else 100
    )

    average_inventory_level = sim_df["soh_end"].mean()

    stock_coverage_days = (
        total_soh / total_demand
        if total_demand > 0 else 0
    )

    return {
        "Stock Cost": total_stock_cost,
        "Order Total Cost": total_order_cost,
        "Total Cost": total_cost,
        "Stock Out Rate (%)": stockout_rate,
        "Alpha Service Level (%)": alpha_service_level,
        "Beta Service Level (%)": beta_service_level,
        "Average Inventory Level": average_inventory_level,
        "Stock Coverage (days)": stock_coverage_days,
        "Total Demand": total_demand,
        "Total Stockout": total_stockout,
        "Total SOH": total_soh,
        "Stockout Days": stockout_days,
        "Total Days": total_days,
    }


# ========================= CARREGAR POLÍTICAS =========================

policy_data = {}

for policy_name, file_name in POLICIES.items():
    path = os.path.join(folder, file_name)

    if not os.path.exists(path):
        raise FileNotFoundError(f"Ficheiro não encontrado: {path}")

    df = load_policy_csv(path)
    df = normalize_policy_df(df)

    required_cols = [
        "sku",
        "date",
        "demand",
        "soh_start",
        "soh_end",
        "orders_placed",
        "avg_lt",
        "std_lt",
        "inventory_holding_cost",
        "order_total_cost",
    ]

    missing = [c for c in required_cols if c not in df.columns]

    if missing:
        raise ValueError(
            f"A política '{policy_name}' não tem estas colunas necessárias: {missing}"
        )

    df = infer_unit_cost(df)

    policy_data[policy_name] = df


# ========================= GARANTIR MESMOS SKUS =========================

common_skus = None

for policy_name, df in policy_data.items():
    skus = set(df["sku"].dropna().unique())
    common_skus = skus if common_skus is None else common_skus & skus

for policy_name in policy_data:
    policy_data[policy_name] = policy_data[policy_name][
        policy_data[policy_name]["sku"].isin(common_skus)
    ].copy()


# ========================= MONTE CARLO =========================

mc_results = []

for policy_name, policy_df in policy_data.items():

    print(f"\nA correr Monte Carlo para: {policy_name}")

    for sim in range(1, N_SIMULATIONS + 1):

        sim_df = simulate_policy_once(
            policy_df=policy_df,
            seed=sim
        )

        kpis = calculate_mc_kpis(sim_df)

        mc_results.append({
            "Policy": policy_name,
            "Simulation": sim,
            **kpis
        })

        if sim % 10 == 0:
            print(f"  Simulação {sim}/{N_SIMULATIONS}")


# ========================= EXPORTAR =========================

mc_df = pd.DataFrame(mc_results)

numeric_cols = [
    "Stock Cost",
    "Order Total Cost",
    "Total Cost",
    "Stock Out Rate (%)",
    "Alpha Service Level (%)",
    "Beta Service Level (%)",
    "Average Inventory Level",
    "Stock Coverage (days)",
    "Total Demand",
    "Total Stockout",
    "Total SOH",
]

for col in numeric_cols:
    if col in mc_df.columns:
        mc_df[col] = pd.to_numeric(mc_df[col], errors="coerce").round(2)

output_path = os.path.join(folder, output_file)

mc_df.to_csv(
    output_path,
    index=False,
    sep=";",
    decimal=",",
    encoding="utf-8-sig"
)

print("\nMonte Carlo concluído.")
print(f"Ficheiro guardado em: {output_path}")
print(f"Políticas simuladas: {mc_df['Policy'].nunique()}")
print(f"Simulações por política: {N_SIMULATIONS}")
print(f"SKUs comuns usados: {len(common_skus)}")
