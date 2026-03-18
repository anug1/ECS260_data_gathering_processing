import json
import pandas as pd

INPUT_FILE  = "repo_creations_ecs260.json"   # 👈 change to your actual filename
OUTPUT_FILE = "repos_table_nonfork.csv"

records = []
errors  = 0

with open(INPUT_FILE, "r") as f:
    for line_num, line in enumerate(f, 1):
        line = line.strip()
        if not line:
            continue
        try:
            row = json.loads(line)

            # Parse payload — may be string or dict
            payload = row.get("payload", {})
            if isinstance(payload, str):
                payload = json.loads(payload)

            repo  = row.get("repo", {})
            actor = row.get("actor", {})
            org   = row.get("org", {})

            repo_name   = repo.get("name")
            created_at  = row.get("created_at")

            # Skip if missing key fields
            if not repo_name or not created_at:
                continue

            # Filter to Dec 2023 and Jan 2024 only
            created_dt = pd.to_datetime(created_at, utc=True, errors="coerce")
            if pd.isna(created_dt):
                continue
            if not ((created_dt.year == 2023 and created_dt.month == 12) or
                    (created_dt.year == 2024 and created_dt.month == 1)):
                continue

            record = {
                "repo_name":        repo_name,
                "repo_id":          repo.get("id"),
                "created_at":       created_at,
                "is_fork":          False,
                "default_branch":   payload.get("master_branch"),
                "fork_owner_login": actor.get("login"),
                "fork_owner_type":  "Organization" if org.get("login") else "User",
                "parent_repo_name": None,
                "parent_repo_id":   None,
                "parent_org":       org.get("login"),
                "event_id":         row.get("id"),
                "event_created_at": created_at,
            }
            records.append(record)

        except (json.JSONDecodeError, KeyError) as e:
            errors += 1
            if errors <= 5:
                print(f"Line {line_num} error: {e}")

print(f"Parsed {len(records)} non-fork repos. Errors: {errors}")

# ---------------------------------------------------------------
# BUILD DATAFRAME & CLEAN
# ---------------------------------------------------------------
df = pd.DataFrame(records)

df["created_at"]       = pd.to_datetime(df["created_at"], utc=True, errors="coerce")
df["event_created_at"] = pd.to_datetime(df["event_created_at"], utc=True, errors="coerce")

# Drop rows with no repo name
df = df.dropna(subset=["repo_name"])

# Remove duplicates — same repo appearing multiple times
df = df.drop_duplicates(subset=["repo_name"])

print(f"\nTotal non-fork repos after dedup: {len(df)}")
print(f"Dec 2023: {(df['created_at'].dt.month == 12).sum()}")
print(f"Jan 2024: {(df['created_at'].dt.month == 1).sum()}")
print(f"User-owned: {(df['fork_owner_type'] == 'User').sum()}")
print(f"Org-owned:  {(df['fork_owner_type'] == 'Organization').sum()}")

df.to_csv(OUTPUT_FILE, index=False)
print(f"\nSaved to {OUTPUT_FILE}")
print("\nSample:")
print(df[["repo_name", "created_at", "fork_owner_type"]].head(10))