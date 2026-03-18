"""
Collects sustainability labels for NON-FORK repos.
A repo is "sustainable" (1) if it had at least 1 commit
between month 18 and month 24 after its creation date.

Repos created Dec 2023 - Jan 2024.
Sustainability window = Jun 2025 - Jan 2026.
"""

import requests, gzip, json, os, time
import pandas as pd
from datetime import datetime, timedelta, timezone
from collections import defaultdict

# ---------------------------------------------------------------
# LOAD YOUR NON-FORK REPOS
# ---------------------------------------------------------------
df = pd.read_csv("final_dataset_nonfork.csv", parse_dates=["created_at"])
df["created_at"] = pd.to_datetime(df["created_at"], utc=True)

repo_lookup = dict(zip(df["repo_name"], df["created_at"]))
repo_set    = set(repo_lookup.keys())

print(f"Loaded {len(repo_set)} non-fork repos to check for sustainability.")

# ---------------------------------------------------------------
# SUSTAINABILITY WINDOW
# Month 18 to month 24 = 548 to 730 days after creation
# ---------------------------------------------------------------
WINDOW_START_DAYS = 548
WINDOW_END_DAYS   = 730

def in_sustainability_window(repo_name, event_time):
    created = repo_lookup.get(repo_name)
    if created is None:
        return False
    if event_time.tzinfo is None:
        event_time = event_time.replace(tzinfo=timezone.utc)
    delta_days = (event_time - created).days
    return WINDOW_START_DAYS <= delta_days <= WINDOW_END_DAYS

# ---------------------------------------------------------------
# ACCUMULATOR
# ---------------------------------------------------------------
has_commit_in_window = defaultdict(bool)

# ---------------------------------------------------------------
# HELPER: process one event
# ---------------------------------------------------------------
def process_event(event):
    if event.get("type") != "PushEvent":
        return

    repo_name = event.get("repo", {}).get("name")
    if repo_name not in repo_set:
        return

    if has_commit_in_window[repo_name]:
        return

    try:
        event_time = datetime.strptime(
            event["created_at"], "%Y-%m-%dT%H:%M:%SZ"
        ).replace(tzinfo=timezone.utc)
    except Exception:
        return

    if in_sustainability_window(repo_name, event_time):
        has_commit_in_window[repo_name] = True

# ---------------------------------------------------------------
# MAIN LOOP: Jun 2025 to Jan 2026
# ---------------------------------------------------------------
start_date = datetime(2025, 6, 1)
end_date   = datetime(2026, 1, 31)

PROGRESS_FILE = "sustainability_progress_nonfork.txt"
done_files    = set()
if os.path.exists(PROGRESS_FILE):
    with open(PROGRESS_FILE) as f:
        done_files = set(f.read().splitlines())
    print(f"Resuming — {len(done_files)} files already processed.")

total_hours = int((end_date - start_date).total_seconds() / 3600)
processed   = 0
errors      = 0
current     = start_date

print(f"\nProcessing {total_hours} hourly files from {start_date.date()} to {end_date.date()}")
print("Expected runtime: 1-3 hours. Progress saved — safe to restart.\n")

while current <= end_date:
    for hour in range(24):
        filename = f"{current.strftime('%Y-%m-%d')}-{hour}.json.gz"
        url      = f"https://data.gharchive.org/{filename}"

        if filename in done_files:
            processed += 1
            continue

        try:
            r = requests.get(url, timeout=60)
            if r.status_code == 404:
                processed += 1
                continue
            r.raise_for_status()

            with gzip.open(__import__('io').BytesIO(r.content)) as f:
                for line in f:
                    try:
                        event = json.loads(line)
                        process_event(event)
                    except json.JSONDecodeError:
                        continue

            with open(PROGRESS_FILE, "a") as pf:
                pf.write(filename + "\n")

            processed += 1
            if processed % 100 == 0:
                pct         = processed / total_hours * 100
                sustainable = sum(has_commit_in_window.values())
                print(f"  Progress: {processed}/{total_hours} files ({pct:.1f}%) | "
                      f"Sustainable so far: {sustainable}")

        except Exception as e:
            errors += 1
            if errors <= 10:
                print(f"  Error on {filename}: {e}")
            time.sleep(2)

    current += timedelta(days=1)

print(f"\nDone! {processed} files processed, {errors} errors.")

# ---------------------------------------------------------------
# BUILD LABELS AND MERGE INTO FINAL DATASET
# ---------------------------------------------------------------
print("\nBuilding sustainability labels...")

labels = []
for repo in repo_set:
    labels.append({
        "repo_name":      repo,
        "is_sustainable": 1 if has_commit_in_window[repo] else 0
    })

df_labels = pd.DataFrame(labels)

sustainable   = df_labels["is_sustainable"].sum()
unsustainable = len(df_labels) - sustainable
print(f"  Sustainable (1):     {sustainable} ({sustainable/len(df_labels)*100:.1f}%)")
print(f"  Not sustainable (0): {unsustainable} ({unsustainable/len(df_labels)*100:.1f}%)")

# Merge into final dataset
df_final = pd.read_csv("final_dataset_nonfork.csv")
df_final = df_final.merge(df_labels, on="repo_name", how="left")

df_final.to_csv("final_dataset_nonfork.csv", index=False)
print(f"\n✅ Saved! final_dataset_nonfork.csv now has {len(df_final)} rows and {len(df_final.columns)} columns.")
print(f"   New column added: 'is_sustainable' (0 or 1)")