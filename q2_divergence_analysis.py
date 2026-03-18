"""
Q2: How does a forked project's divergence from its parent
affect its long-term sustainability?
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import classification_report
import warnings
warnings.filterwarnings("ignore")

# ---------------------------------------------------------------
# 1. LOAD AND PREPARE DATA
# ---------------------------------------------------------------
print("Loading data...")
df = pd.read_csv("final_dataset.csv")

# Keep only forks with divergence ratio available
df_div = df[df["divergence_ratio"].notna()].copy()

print(f"Total forks:                    {len(df)}")
print(f"Forks with divergence ratio:    {len(df_div)}")
print(f"Missing divergence ratio:       {df['divergence_ratio'].isna().sum()}")
print(f"\nSustainability rate (all forks):         {df['is_sustainable'].mean()*100:.1f}%")
print(f"Sustainability rate (with divergence):   {df_div['is_sustainable'].mean()*100:.1f}%")

# ---------------------------------------------------------------
# 2. DESCRIPTIVE ANALYSIS — GROUP BY DIVERGENCE LEVEL
# ---------------------------------------------------------------
print("\n=== Descriptive Analysis ===")

# Create divergence groups
bins   = [0, 0.33, 0.66, 1.01]
labels = ["Low (0.0-0.33)", "Medium (0.33-0.66)", "High (0.66-1.0)"]
df_div["divergence_group"] = pd.cut(df_div["divergence_ratio"], bins=bins, labels=labels)

group_stats = df_div.groupby("divergence_group").agg(
    count          = ("repo_name", "count"),
    sustainable    = ("is_sustainable", "sum"),
    sustainability_rate = ("is_sustainable", "mean"),
    avg_divergence = ("divergence_ratio", "mean")
).reset_index()

group_stats["sustainability_rate"] = group_stats["sustainability_rate"] * 100
print(group_stats.to_string(index=False))

# ---------------------------------------------------------------
# 3. VISUALISATION
# ---------------------------------------------------------------
print("\nGenerating plots...")
fig, axes = plt.subplots(1, 3, figsize=(18, 6))

# Plot 1: Sustainability rate by divergence group
axes[0].bar(
    group_stats["divergence_group"],
    group_stats["sustainability_rate"],
    color=["#d9534f", "#f0ad4e", "#5cb85c"]
)
axes[0].set_title("Sustainability Rate by Divergence Group")
axes[0].set_xlabel("Divergence Group")
axes[0].set_ylabel("Sustainability Rate (%)")
for i, v in enumerate(group_stats["sustainability_rate"]):
    axes[0].text(i, v + 0.1, f"{v:.1f}%", ha="center", fontsize=10)

# Plot 2: Violin plot — divergence ratio by sustainability
sustainable_vals   = df_div[df_div["is_sustainable"] == 1]["divergence_ratio"]
unsustainable_vals = df_div[df_div["is_sustainable"] == 0]["divergence_ratio"]
axes[1].violinplot([unsustainable_vals, sustainable_vals], positions=[0, 1])
axes[1].set_xticks([0, 1])
axes[1].set_xticklabels(["Not Sustainable (0)", "Sustainable (1)"])
axes[1].set_title("Divergence Ratio Distribution\nby Sustainability")
axes[1].set_ylabel("Divergence Ratio")

# Plot 3: Box plot
df_div["sustainability_label"] = df_div["is_sustainable"].map(
    {0: "Not Sustainable", 1: "Sustainable"}
)
axes[2].boxplot(
    [unsustainable_vals, sustainable_vals],
    labels=["Not Sustainable", "Sustainable"],
    patch_artist=True,
    boxprops=dict(facecolor="#aec6cf")
)
axes[2].set_title("Divergence Ratio Box Plot\nby Sustainability")
axes[2].set_ylabel("Divergence Ratio")

plt.suptitle("Q2: How Does Divergence Ratio Affect Sustainability?", fontsize=14)
plt.tight_layout()
plt.savefig("q2_divergence_analysis.png", dpi=150, bbox_inches="tight")
plt.show()
print("Saved q2_divergence_analysis.png")

# ---------------------------------------------------------------
# 4. MANN-WHITNEY U TEST
# ---------------------------------------------------------------
print("\n=== Mann-Whitney U Test ===")
stat, p_value = stats.mannwhitneyu(sustainable_vals, unsustainable_vals, alternative="two-sided")

print(f"Sustainable repos    — mean divergence: {sustainable_vals.mean():.4f}, median: {sustainable_vals.median():.4f}")
print(f"Non-sustainable repos — mean divergence: {unsustainable_vals.mean():.4f}, median: {unsustainable_vals.median():.4f}")
print(f"Mann-Whitney U statistic: {stat:.2f}")
print(f"P-value: {p_value:.6f}")
if p_value < 0.05:
    print("✅ Statistically significant difference (p < 0.05)")
else:
    print("❌ No statistically significant difference (p >= 0.05)")

# ---------------------------------------------------------------
# 5. POINT-BISERIAL CORRELATION
# ---------------------------------------------------------------
print("\n=== Point-Biserial Correlation ===")
corr, p_corr = stats.pointbiserialr(df_div["is_sustainable"], df_div["divergence_ratio"])
print(f"Correlation: {corr:.4f}")
print(f"P-value:     {p_corr:.6f}")
if p_corr < 0.05:
    print("✅ Statistically significant correlation (p < 0.05)")
else:
    print("❌ No statistically significant correlation (p >= 0.05)")

# ---------------------------------------------------------------
# 6. SIMPLE LOGISTIC REGRESSION
# (divergence ratio only)
# ---------------------------------------------------------------
print("\n=== Simple Logistic Regression (divergence ratio only) ===")

X_simple = df_div[["divergence_ratio"]].values
y        = df_div["is_sustainable"].values

scaler   = StandardScaler()
X_scaled = scaler.fit_transform(X_simple)

lr_simple = LogisticRegression(class_weight="balanced", random_state=42)
lr_simple.fit(X_scaled, y)

coef = lr_simple.coef_[0][0]
odds_ratio = np.exp(coef)
print(f"Coefficient:  {coef:.4f}")
print(f"Odds Ratio:   {odds_ratio:.4f}")
print(f"Interpretation: A 1 standard deviation increase in divergence ratio")
print(f"multiplies the odds of sustainability by {odds_ratio:.4f}")

# ---------------------------------------------------------------
# 7. LOGISTIC REGRESSION WITH CONTROLS
# ---------------------------------------------------------------
print("\n=== Logistic Regression with Controls ===")

control_cols = [
    "divergence_ratio",
    "total_commits",
    "unique_commit_authors",
    "star_count",
    "has_license",
    "has_cicd",
    "fork_owner_type"
]

df_ctrl = df_div[control_cols + ["is_sustainable"]].copy()

# Encode fork_owner_type
df_ctrl["fork_owner_type"] = (df_ctrl["fork_owner_type"] == "Organization").astype(int)

# Encode booleans
for col in ["has_license", "has_cicd"]:
    df_ctrl[col] = df_ctrl[col].astype(int)

# Fill missing
df_ctrl = df_ctrl.fillna(0)

X_ctrl   = df_ctrl[control_cols].values
y_ctrl   = df_ctrl["is_sustainable"].values

X_ctrl_scaled = scaler.fit_transform(X_ctrl)

lr_ctrl = LogisticRegression(class_weight="balanced", random_state=42, max_iter=1000)
lr_ctrl.fit(X_ctrl_scaled, y_ctrl)

coef_df = pd.DataFrame({
    "feature":    control_cols,
    "coefficient": lr_ctrl.coef_[0],
    "odds_ratio":  np.exp(lr_ctrl.coef_[0])
}).sort_values("odds_ratio", ascending=False)

print("\nLogistic Regression Coefficients (with controls):")
print(coef_df.to_string(index=False))

print(f"\nDivergence ratio odds ratio (with controls): {coef_df[coef_df['feature'] == 'divergence_ratio']['odds_ratio'].values[0]:.4f}")
print("If odds ratio > 1: higher divergence → more likely sustainable")
print("If odds ratio < 1: higher divergence → less likely sustainable")

# ---------------------------------------------------------------
# 8. SAVE RESULTS
# ---------------------------------------------------------------
group_stats.to_csv("q2_group_stats.csv", index=False)
coef_df.to_csv("q2_logistic_regression_results.csv", index=False)
print("\n✅ Results saved to q2_group_stats.csv and q2_logistic_regression_results.csv")