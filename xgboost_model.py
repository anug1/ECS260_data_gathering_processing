"""
XGBoost models to predict sustainability for forks and non-forks separately.
Uses proper 5-fold cross validation with SMOTE applied inside each fold
to avoid data leakage. Compares feature importances between the two groups.
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.model_selection import StratifiedKFold
from sklearn.metrics import (roc_auc_score, classification_report,
                             confusion_matrix)
from sklearn.preprocessing import LabelEncoder
from imblearn.over_sampling import SMOTE
from imblearn.pipeline import Pipeline
import xgboost as xgb
import warnings
warnings.filterwarnings("ignore")

# ---------------------------------------------------------------
# 1. LOAD DATA
# ---------------------------------------------------------------
print("Loading datasets...")
df_fork    = pd.read_csv("final_dataset.csv")
df_nonfork = pd.read_csv("final_dataset_nonfork.csv")

print(f"Forks:     {len(df_fork)} rows")
print(f"Non-forks: {len(df_nonfork)} rows")

# ---------------------------------------------------------------
# 2. DEFINE FEATURES
# ---------------------------------------------------------------
DROP_ALWAYS = [
    "repo_name", "created_at", "parent_repo", "parent_created_at",
    "topics", "is_fork", "is_sustainable"
]

FORK_ONLY_COLS = [
    "parent_stars", "parent_forks", "parent_language",
    "early_commits_on_branch", "parent_early_commits", "divergence_ratio"
]

# ---------------------------------------------------------------
# 3. PREPROCESSING FUNCTION
# ---------------------------------------------------------------
def preprocess(df, is_fork_model=True):
    df = df.copy()

    drop_cols = DROP_ALWAYS.copy()
    if not is_fork_model:
        drop_cols += FORK_ONLY_COLS

    feature_cols = [c for c in df.columns if c not in drop_cols]
    X = df[feature_cols].copy()
    y = df["is_sustainable"].copy()

    # Convert booleans to int
    bool_cols = X.select_dtypes(include="bool").columns
    X[bool_cols] = X[bool_cols].astype(int)

    # Encode categoricals
    cat_cols = X.select_dtypes(include="object").columns
    for col in cat_cols:
        le = LabelEncoder()
        X[col] = X[col].astype(str)
        X[col] = le.fit_transform(X[col])

    # Fill missing values
    for col in ["avg_issue_first_response_hrs", "avg_pr_first_response_hrs"]:
        if col in X.columns:
            X[col] = X[col].fillna(X[col].median() * 2)

    for col in ["pr_acceptance_rate", "issue_close_rate",
                "retention_m3", "retention_m6", "contributor_gini",
                "divergence_ratio", "months_to_first_external_contrib"]:
        if col in X.columns:
            X[col] = X[col].fillna(0)

    X = X.fillna(0)

    return X, y, feature_cols

# ---------------------------------------------------------------
# 4. TRAIN WITH PROPER K-FOLD + SMOTE INSIDE EACH FOLD
# ---------------------------------------------------------------
def train_kfold_xgboost(X, y, label, n_splits=5):
    print(f"\n{'='*60}")
    print(f"Training XGBoost with {n_splits}-Fold CV — {label}")
    print(f"  Total samples:   {len(y)}")
    print(f"  Sustainable (1): {y.sum()} ({y.mean()*100:.1f}%)")
    print(f"{'='*60}")

    scale_pos_weight = (y == 0).sum() / (y == 1).sum()

    skf = StratifiedKFold(n_splits=n_splits, shuffle=True, random_state=42)

    # Storage across folds
    fold_aucs        = []
    fold_importances = []
    all_y_true       = []
    all_y_pred_proba = []
    all_y_pred       = []

    X_arr = X.values
    y_arr = y.values

    for fold, (train_idx, test_idx) in enumerate(skf.split(X_arr, y_arr), 1):
        X_train, X_test = X_arr[train_idx], X_arr[test_idx]
        y_train, y_test = y_arr[train_idx], y_arr[test_idx]

        # Apply SMOTE inside the fold — avoids data leakage
        smote = SMOTE(random_state=42, k_neighbors=min(5, y_train.sum() - 1))
        X_train_sm, y_train_sm = smote.fit_resample(X_train, y_train)

        # Train XGBoost
        model = xgb.XGBClassifier(
            n_estimators=500,
            max_depth=5,
            learning_rate=0.05,
            subsample=0.8,
            colsample_bytree=0.8,
            min_child_weight=5,
            gamma=0.1,
            reg_alpha=0.1,
            reg_lambda=1.0,
            scale_pos_weight=scale_pos_weight,
            eval_metric="auc",
            random_state=42,
            n_jobs=-1,
            verbosity=0
        )
        model.fit(X_train_sm, y_train_sm)

        # Predict on test fold
        y_proba = model.predict_proba(X_test)[:, 1]
        y_pred  = model.predict(X_test)
        auc     = roc_auc_score(y_test, y_proba)

        fold_aucs.append(auc)
        fold_importances.append(model.feature_importances_)
        all_y_true.extend(y_test)
        all_y_pred_proba.extend(y_proba)
        all_y_pred.extend(y_pred)

        print(f"  Fold {fold}: AUC = {auc:.4f}")

    # Average results across folds
    mean_auc = np.mean(fold_aucs)
    std_auc  = np.std(fold_aucs)
    mean_importances = np.mean(fold_importances, axis=0)

    print(f"\n=== Results: {label} ===")
    print(f"Mean AUC-ROC: {mean_auc:.4f} ± {std_auc:.4f}")
    print("\nClassification Report (aggregated across all folds):")
    print(classification_report(
        all_y_true, all_y_pred,
        target_names=["Not Sustainable", "Sustainable"]
    ))

    return mean_auc, std_auc, mean_importances

# ---------------------------------------------------------------
# 5. FEATURE IMPORTANCE FUNCTION
# ---------------------------------------------------------------
def get_feature_importance(mean_importances, feature_cols, label, top_n=15):
    importance = pd.DataFrame({
        "feature":    feature_cols,
        "importance": mean_importances
    }).sort_values("importance", ascending=False).head(top_n)

    print(f"\n=== Top {top_n} Features: {label} ===")
    for _, row in importance.iterrows():
        bar = "█" * int(row["importance"] * 200)
        print(f"  {row['feature']:<40} {row['importance']:.4f} {bar}")

    return importance

# ---------------------------------------------------------------
# 6. RUN FORK MODEL
# ---------------------------------------------------------------
X_fork, y_fork, fork_features = preprocess(df_fork, is_fork_model=True)
auc_fork, std_fork, imp_fork  = train_kfold_xgboost(X_fork, y_fork, "FORKS")
importance_fork = get_feature_importance(imp_fork, fork_features, "FORKS")

# ---------------------------------------------------------------
# 7. RUN NON-FORK MODEL
# ---------------------------------------------------------------
X_nonfork, y_nonfork, nonfork_features = preprocess(df_nonfork, is_fork_model=False)
auc_nonfork, std_nonfork, imp_nonfork  = train_kfold_xgboost(X_nonfork, y_nonfork, "NON-FORKS")
importance_nonfork = get_feature_importance(imp_nonfork, nonfork_features, "NON-FORKS")

# ---------------------------------------------------------------
# 8. COMPARISON PLOT
# ---------------------------------------------------------------
print("\nGenerating comparison plot...")

fig, axes = plt.subplots(1, 2, figsize=(18, 8))

top_fork = importance_fork.head(15)
axes[0].barh(top_fork["feature"][::-1], top_fork["importance"][::-1], color="steelblue")
axes[0].set_title(f"Top 15 Features — FORKS\nAUC-ROC: {auc_fork:.4f} ± {std_fork:.4f}", fontsize=14)
axes[0].set_xlabel("Feature Importance (avg across 5 folds)")
axes[0].tick_params(axis="y", labelsize=10)

top_nonfork = importance_nonfork.head(15)
axes[1].barh(top_nonfork["feature"][::-1], top_nonfork["importance"][::-1], color="darkorange")
axes[1].set_title(f"Top 15 Features — NON-FORKS\nAUC-ROC: {auc_nonfork:.4f} ± {std_nonfork:.4f}", fontsize=14)
axes[1].set_xlabel("Feature Importance (avg across 5 folds)")
axes[1].tick_params(axis="y", labelsize=10)

plt.suptitle("Early Predictors of OSS Sustainability: Forks vs Non-Forks", fontsize=16)
plt.tight_layout()
plt.savefig("feature_importance_comparison_kfold.png", dpi=150, bbox_inches="tight")
plt.show()
print("Saved to feature_importance_comparison_kfold.png")

# ---------------------------------------------------------------
# 9. SAVE RESULTS
# ---------------------------------------------------------------
importance_fork.to_csv("feature_importance_forks_kfold.csv", index=False)
importance_nonfork.to_csv("feature_importance_nonfork_kfold.csv", index=False)

# ---------------------------------------------------------------
# 10. FINAL SUMMARY
# ---------------------------------------------------------------
print("\n" + "="*60)
print("FINAL SUMMARY")
print("="*60)
print(f"Fork model AUC-ROC:     {auc_fork:.4f} ± {std_fork:.4f}")
print(f"Non-fork model AUC-ROC: {auc_nonfork:.4f} ± {std_nonfork:.4f}")

print("\nTop 5 features for FORKS:")
for _, r in importance_fork.head(5).iterrows():
    print(f"  {r['feature']:<40} {r['importance']:.4f}")

print("\nTop 5 features for NON-FORKS:")
for _, r in importance_nonfork.head(5).iterrows():
    print(f"  {r['feature']:<40} {r['importance']:.4f}")

print("\nOutput files:")
print("  feature_importance_comparison_kfold.png")
print("  feature_importance_forks_kfold.csv")
print("  feature_importance_nonfork_kfold.csv")