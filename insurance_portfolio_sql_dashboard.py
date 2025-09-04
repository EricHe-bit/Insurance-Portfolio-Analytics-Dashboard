# Insurance Portfolio Analytics Dashboard - SQL-first implementation
# Creates a SQLite database file, populates Policies and Claims tables, runs SQL analytics,
# exports CSVs, and generates matplotlib plots (one per figure, no seaborn, no custom colors).

import sqlite3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from math import log

# --- Setup ---
rng = np.random.default_rng(42)
DB_PATH = "data/insurance_portfolio.db"

# Remove existing DB if exists (safe in this environment)
import os
if os.path.exists(DB_PATH):
    os.remove(DB_PATH)

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

# --- Create schema: Policies and Claims ---
cur.executescript("""
CREATE TABLE Policies (
    policy_id INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_age INTEGER NOT NULL,
    car_type TEXT NOT NULL,
    premium REAL NOT NULL
);

CREATE TABLE Claims (
    claim_id INTEGER PRIMARY KEY AUTOINCREMENT,
    policy_id INTEGER NOT NULL,
    claim_amount REAL NOT NULL,
    claim_date TEXT,
    FOREIGN KEY(policy_id) REFERENCES Policies(policy_id)
);

CREATE INDEX idx_claims_policy ON Claims(policy_id);
""")
conn.commit()

# --- Generate mock policies ---
N_POLICIES = 1000
ages = rng.integers(18, 80, size=N_POLICIES)
car_types = rng.choice(["Sedan", "SUV", "Truck", "Sports"], size=N_POLICIES, p=[0.4, 0.3, 0.2, 0.1])
premiums = rng.normal(1200, 250, size=N_POLICIES).clip(400, 4000).round(2)

policy_rows = [(int(ages[i]), str(car_types[i]), float(premiums[i])) for i in range(N_POLICIES)]
cur.executemany("INSERT INTO Policies (customer_age, car_type, premium) VALUES (?,?,?)", policy_rows)
conn.commit()

# --- Generate mock claims (separate table) ---
# We'll assign a per-policy claim frequency based on age and car type, then draw claim counts from Poisson.
claims_rows = []

# severity distribution: use lognormal to allow heavy tails
sigma = 0.9
mu = log(7000) - 0.5*(sigma**2)

cur.execute("SELECT policy_id, customer_age, car_type FROM Policies")
policy_info = cur.fetchall()

for pid, age, car in policy_info:
    # base frequency (expected claims per year)
    base_lambda = 0.12  # low base frequency per policy
    if car == "Sports":
        base_lambda *= 2.0
    elif car == "Truck":
        base_lambda *= 1.4
    if age < 25:
        base_lambda *= 1.6
    # draw claim count
    n_claims = rng.poisson(lam=base_lambda)
    for _ in range(n_claims):
        # lognormal severity
        amt = float(rng.lognormal(mu, sigma))
        claims_rows.append((pid, round(amt, 2), None))

# Bulk insert claims
if claims_rows:
    cur.executemany("INSERT INTO Claims (policy_id, claim_amount, claim_date) VALUES (?,?,?)", claims_rows)
    conn.commit()

# --- SQL analytics queries ---

# 1) Loss ratio and counts by car type
q_loss_by_car = """
SELECT p.car_type AS car_type,
       COUNT(DISTINCT p.policy_id) AS num_policies,
       COALESCE(SUM(c.claim_amount),0.0) AS total_claims,
       SUM(p.premium) AS total_premiums,
       CASE WHEN SUM(p.premium) = 0 THEN NULL ELSE COALESCE(SUM(c.claim_amount),0.0) * 1.0 / SUM(p.premium) END AS loss_ratio,
       COUNT(c.claim_id) AS total_claims_count
FROM Policies p
LEFT JOIN Claims c ON p.policy_id = c.policy_id
GROUP BY p.car_type
ORDER BY loss_ratio DESC
"""
loss_by_car = pd.read_sql_query(q_loss_by_car, conn)

# 2) Age-group level stats: avg claims per policy, total claims amount, total premiums -> use CTE to compute per-policy totals first
q_age_groups = """
WITH policy_claims AS (
  SELECT p.policy_id,
         p.customer_age,
         p.premium,
         COALESCE(COUNT(c.claim_id), 0) AS claims_count,
         COALESCE(SUM(c.claim_amount), 0.0) AS claims_amount
  FROM Policies p
  LEFT JOIN Claims c ON p.policy_id = c.policy_id
  GROUP BY p.policy_id, p.customer_age, p.premium
)
SELECT
  CASE
    WHEN customer_age BETWEEN 18 AND 29 THEN '18-29'
    WHEN customer_age BETWEEN 30 AND 39 THEN '30-39'
    WHEN customer_age BETWEEN 40 AND 49 THEN '40-49'
    WHEN customer_age BETWEEN 50 AND 59 THEN '50-59'
    WHEN customer_age BETWEEN 60 AND 69 THEN '60-69'
    ELSE '70+'
  END AS age_group,
  COUNT(*) AS num_policies,
  AVG(claims_count) AS avg_claims_per_policy,
  SUM(claims_amount) AS total_claims_amount,
  SUM(premium) AS total_premiums,
  CASE WHEN SUM(premium)=0 THEN NULL ELSE SUM(claims_amount)*1.0 / SUM(premium) END AS loss_ratio
FROM policy_claims
GROUP BY age_group
ORDER BY age_group
"""
age_group_stats = pd.read_sql_query(q_age_groups, conn)

# 3) Top 10 policies by total claims amount
q_top_policies = """
SELECT p.policy_id, p.customer_age, p.car_type, p.premium, COALESCE(SUM(c.claim_amount),0.0) AS total_claims_amount, COUNT(c.claim_id) AS claims_count
FROM Policies p
LEFT JOIN Claims c ON p.policy_id = c.policy_id
GROUP BY p.policy_id
ORDER BY total_claims_amount DESC
LIMIT 10
"""
top_policies = pd.read_sql_query(q_top_policies, conn)

# 4) Portfolio mix by car type (counts)
q_portfolio_mix = """
SELECT car_type, COUNT(*) AS num_policies
FROM Policies
GROUP BY car_type
"""
portfolio_mix = pd.read_sql_query(q_portfolio_mix, conn)

# 5) Per-policy total claims distribution for histogram (we'll compute percentiles in pandas)
q_per_policy = """
SELECT p.policy_id, p.customer_age, p.car_type, p.premium, COALESCE(SUM(c.claim_amount),0.0) AS total_claims_amount, COUNT(c.claim_id) AS claims_count
FROM Policies p
LEFT JOIN Claims c ON p.policy_id = c.policy_id
GROUP BY p.policy_id
"""
per_policy = pd.read_sql_query(q_per_policy, conn)

# --- Export CSVs for your repo/portfolio ---
out_loss_by_car = "data/loss_by_car.csv"
out_age_groups = "data/age_group_stats.csv"
out_top_policies = "data/top_policies.csv"
out_portfolio_mix = "data/portfolio_mix.csv"
out_per_policy = "data/per_policy.csv"

loss_by_car.to_csv(out_loss_by_car, index=False)
age_group_stats.to_csv(out_age_groups, index=False)
top_policies.to_csv(out_top_policies, index=False)
portfolio_mix.to_csv(out_portfolio_mix, index=False)
per_policy.to_csv(out_per_policy, index=False)

# --- Plotting (matplotlib only; one figure per plot) ---
# 1: Loss ratio by car type (bar)
plt.figure()
plt.bar(loss_by_car['car_type'], loss_by_car['loss_ratio'])
plt.title("Loss Ratio by Car Type")
plt.xlabel("Car Type")
plt.ylabel("Loss Ratio (Claims / Premiums)")
plt.tight_layout()
plt.show()

# 2: Avg claims per policy by age group (line)
plt.figure()
# Ensure age_group order is correct
age_group_stats['age_group'] = pd.Categorical(age_group_stats['age_group'], categories=['18-29','30-39','40-49','50-59','60-69','70+'], ordered=True)
age_group_stats = age_group_stats.sort_values('age_group')
plt.plot(age_group_stats['age_group'].astype(str), age_group_stats['avg_claims_per_policy'], marker='o')
plt.title("Average Claims per Policy by Age Group")
plt.xlabel("Age Group")
plt.ylabel("Average Claims per Policy")
plt.tight_layout()
plt.show()

# 3: Portfolio mix (pie)
plt.figure()
plt.pie(portfolio_mix['num_policies'], labels=portfolio_mix['car_type'], autopct='%1.1f%%')
plt.title("Portfolio Mix by Car Type")
plt.tight_layout()
plt.show()

# 4: Histogram of per-policy total claims amount
plt.figure()
plt.hist(per_policy['total_claims_amount'], bins=50)
plt.title("Distribution of Total Claims per Policy")
plt.xlabel("Total Claims Amount")
plt.ylabel("Count of Policies")
plt.tight_layout()
plt.show()

# 5: Scatter: premium vs total claims (to spot outliers)
plt.figure()
plt.scatter(per_policy['premium'], per_policy['total_claims_amount'], alpha=0.6)
plt.title("Policy Premium vs Total Claims Amount")
plt.xlabel("Premium ($)")
plt.ylabel("Total Claims Amount ($)")
plt.tight_layout()
plt.show()

# --- Summary metrics (pandas) ---
summary_metrics = {
    'total_policies': int(per_policy.shape[0]),
    'total_claims_records': int(len(claims_rows)),
    'total_claims_amount': float(per_policy['total_claims_amount'].sum()),
    'average_loss_ratio_overall': float(per_policy['total_claims_amount'].sum() / per_policy['premium'].sum())
}
summary_df = pd.DataFrame([summary_metrics])
summary_csv = "data/portfolio_summary_metrics.csv"
summary_df.to_csv(summary_csv, index=False)

# Save the DB file path and scripts for download
script_path = "data/insurance_portfolio_sql_dashboard.py"
with open(script_path, "w") as f:
    f.write("# This file mirrors the notebook/script used to create the SQLite DB and run SQL analytics.\n# See the repository for full details and README.\n")

# Display a few outputs to the user
from caas_jupyter_tools import display_dataframe_to_user
display_dataframe_to_user("Loss Ratio by Car Type", loss_by_car.head(20))
display_dataframe_to_user("Age Group Stats", age_group_stats)
display_dataframe_to_user("Top 10 Policies by Claims Amount", top_policies)

(DB_PATH, out_loss_by_car, out_age_groups, out_top_policies, out_portfolio_mix, out_per_policy, summary_csv, script_path)
