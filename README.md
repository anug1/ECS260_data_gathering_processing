# Early Signals of Success: Forecasting Sustainability in OSS Forks

This project investigates early participation metrics that predict long-term sustainability in forked and non-forked GitHub repositories.

---

## Requirements

### Python Version
Python 3.10 or higher is recommended. A virtual environment is strongly advised.

### Setup
```bash
python3 -m venv oss_env
source oss_env/bin/activate
pip install --upgrade pip
pip install requests pandas numpy scipy scikit-learn imbalanced-learn xgboost matplotlib seaborn
```

---

## Input Files Required

Before running any scripts, you need two raw JSON files:
- `forked.json` — 50,000 forked repository events (one JSON object per line)
- `repo_creations_ecs260.json` — 50,000 non-forked repository creation events (one JSON object per line)

---

## Pipeline Overview

```
forked.json                          repo_creations_ecs260.json
      ↓                                          ↓
50Kfilter2.py                        parse_nonfork_events.py
      ↓                                          ↓
repos_table.csv                      repos_table_nonfork.csv
      ↓                                          ↓
step2_local.py                       nonfork_step2_local.py
      ↓                                          ↓
step2b/c/d_activity/                 step2b/c/d_*_nonfork.csv
responsiveness/contributors.csv
      ↓                                          ↓
step3_graphql.py                     nonfork_step3_graphql.py
      ↓                                          ↓
step3_governance_metadata.csv        step3_governance_metadata_nonfork.csv
      ↓                                          ↓
step4_derived.py                     nonfork_step4_derived.py
      ↓                                          ↓
final_dataset.csv                    final_dataset_nonfork.csv
      ↓                                          ↓
sustainability_labels.py             nonfork_sustainability_labels.py
      ↓                                          ↓
final_dataset.csv                    final_dataset_nonfork.csv
(with is_sustainable column)         (with is_sustainable column)
      ↓                                          ↓
                  xgboost_model.py
                  q2_divergence_analysis.py
```

---

## Step-by-Step Instructions

### Step 0A — Parse Forked Repos
```bash
python 50Kfilter2.py
```
- Input: `forked.json`
- Output: `repos_table.csv`
- Filters out archived repos and empty forks

---

### Step 0B — Parse Non-Forked Repos
```bash
python parse_nonfork_events.py
```
- Input: `repo_creations_ecs260.json`
- Output: `repos_table_nonfork.csv`
- Filters to only repository creation events in Dec 2023 and Jan 2024

---

### Step 2A — GH Archive Download (Forks)
```bash
python step2_local.py
```
- Input: `repos_table.csv`
- Output: `step2b_activity.csv`, `step2c_responsiveness.csv`, `step2d_contributors.csv`
- Downloads GH Archive hourly files from Dec 2023 to Jul 2024
- **Runtime: ~12 hours. Has resume support via `progress.txt`**

---

### Step 2B — GH Archive Download (Non-Forks)
```bash
python nonfork_step2_local.py
```
- Input: `repos_table_nonfork.csv`
- Output: `step2b_activity_nonfork.csv`, `step2c_responsiveness_nonfork.csv`, `step2d_contributors_nonfork.csv`
- **Runtime: ~12 hours. Has resume support via `progress_nonfork.txt`**

---

### Step 3A — GitHub GraphQL API (Forks)
Requires a GitHub personal access token. Replace `YOUR_GITHUB_TOKEN` in `step3_graphql.py`.
```bash
python step3_graphql.py
```
- Input: `step2b_activity.csv`, `repos_table.csv`
- Output: `step3_governance_metadata.csv`
- Collects governance files, CI/CD, parent metadata, divergence ratio
- **Runtime: ~32 hours. Has resume support via `step3_progress.txt`**

---

### Step 3B — GitHub GraphQL API (Non-Forks)
Replace `YOUR_GITHUB_TOKEN` in `nonfork_step3_graphql.py`.
```bash
python nonfork_step3_graphql.py
```
- Input: `step2b_activity_nonfork.csv`, `repos_table_nonfork.csv`
- Output: `step3_governance_metadata_nonfork.csv`
- **Runtime: ~10 hours. Has resume support via `step3_progress_nonfork.txt`**

---

### Step 4A — Derived Metrics (Forks)
```bash
python step4_derived.py
```
- Input: all step2 and step3 fork outputs
- Output: `final_dataset.csv`
- Computes bus factor, gini, retention, PR acceptance rate, issue close rate
- **Runtime: ~5 minutes**

---

### Step 4B — Derived Metrics (Non-Forks)
```bash
python nonfork_step4_derived.py
```
- Input: all step2 and step3 non-fork outputs
- Output: `final_dataset_nonfork.csv`
- **Runtime: ~5 minutes**

---

### Sustainability Labels (Forks)
```bash
python sustainability_labels.py
```
- Input: `final_dataset.csv`
- Output: `final_dataset.csv` with `is_sustainable` column added
- Downloads GH Archive files from Jun 2025 to Jan 2026
- **Runtime: ~2 hours. Has resume support via `sustainability_progress.txt`**

---

### Sustainability Labels (Non-Forks)
```bash
python nonfork_sustainability_labels.py
```
- Input: `final_dataset_nonfork.csv`
- Output: `final_dataset_nonfork.csv` with `is_sustainable` column added
- **Runtime: ~2 hours. Has resume support via `sustainability_progress_nonfork.txt`**

---

## Analysis Scripts

### XGBoost Model (RQ1)
```bash
python xgboost_model.py
```
- Input: `final_dataset.csv`, `final_dataset_nonfork.csv`
- Output: `feature_importance_comparison_kfold.png`, `feature_importance_forks_kfold.csv`, `feature_importance_nonfork_kfold.csv`
- Trains separate XGBoost models for forks and non-forks using 5-fold CV with SMOTE
- **Runtime: ~15-20 minutes**

---

### Divergence Analysis (RQ2)
```bash
python q2_divergence_analysis.py
```
- Input: `final_dataset.csv`
- Output: `q2_divergence_analysis.png`, `q2_group_stats.csv`, `q2_logistic_regression_results.csv`
- Runs Mann-Whitney U test, point-biserial correlation, and logistic regression
- **Runtime: ~1 minute**

---

## Notes

- All long-running scripts have **resume support** — if a script stops for any reason, simply run it again and it will pick up where it left off
- Never close the terminal window while a script is running
- Keep your Mac plugged in and set to stay awake during overnight runs
- GitHub tokens should never be committed to version control — keep them private
- The GitHub GraphQL API allows 5,000 requests per hour. The scripts handle rate limiting automatically