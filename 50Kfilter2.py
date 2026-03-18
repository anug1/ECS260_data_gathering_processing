import json
import pandas as pd

# ---------------------------------------------------------------
# LOAD & PARSE FORK EVENTS
# Your file has one JSON object per line (newline-delimited JSON)
# ---------------------------------------------------------------
INPUT_FILE = "forked.json"  # your 50k fork events file
OUTPUT_FILE = "repos_table.csv"  # output for Step 2 & 3

records = []
errors = 0

with open(INPUT_FILE, "r") as f:
    for line_num, line in enumerate(f, 1):
        line = line.strip()
        if not line:
            continue
        try:
            event = json.loads(line)

            # Parse payload — it may be a string or already a dict
            payload = event.get("payload", {})
            if isinstance(payload, str):
                payload = json.loads(payload)

            forkee = payload.get("forkee", {})
            owner  = forkee.get("owner", {})
            repo   = event.get("repo", {})
            actor  = event.get("actor", {})
            org    = event.get("org", {})

            record = {
                # --- Fork repo (your main subject) ---
                "repo_name":          forkee.get("full_name"),
                "repo_id":            forkee.get("id"),
                "created_at":         forkee.get("created_at"),
                "is_fork":            True,
                "default_branch":     forkee.get("default_branch"),
                "fork_owner_login":   owner.get("login"),
                "fork_owner_type":    owner.get("type"),       # "User" or "Organization"

                # --- Parent repo ---
                "parent_repo_name":   repo.get("name"),
                "parent_repo_id":     repo.get("id"),
                "parent_org":         org.get("login"),        # org if parent belongs to one

                # --- Fork baseline state at time of fork ---
                "initial_stars":      forkee.get("stargazers_count", 0),
                "initial_forks":      forkee.get("forks_count", 0),
                "initial_open_issues":forkee.get("open_issues_count", 0),
                "initial_size_kb":    forkee.get("size", 0),   # repo size in KB at fork time
                "has_issues_enabled": forkee.get("has_issues", False),
                "has_wiki":           forkee.get("has_wiki", False),
                "has_discussions":    forkee.get("has_discussions", False),
                "is_archived":        forkee.get("archived", False),
                "is_template":        forkee.get("is_template", False),
                "initial_language":   forkee.get("language"),  # language at fork time
                "initial_topics":     ",".join(forkee.get("topics", [])),

                # --- Event metadata ---
                "event_id":           event.get("id"),
                "event_created_at":   event.get("created_at"),
            }
            records.append(record)

        except (json.JSONDecodeError, KeyError) as e:
            errors += 1
            if errors <= 5:  # only print first few errors
                print(f"Line {line_num} error: {e}")

print(f"Parsed {len(records)} fork events. Errors: {errors}")

# ---------------------------------------------------------------
# BUILD DATAFRAME & CLEAN
# ---------------------------------------------------------------
df = pd.DataFrame(records)

# Convert timestamps
df["created_at"]       = pd.to_datetime(df["created_at"], utc=True, errors="coerce")
df["event_created_at"] = pd.to_datetime(df["event_created_at"], utc=True, errors="coerce")

# Drop rows with no repo name (can't use these)
df = df.dropna(subset=["repo_name"])

# Drop forks with no activity ever (archived at fork time, or disabled)
df = df[~df["is_archived"]]

# ---------------------------------------------------------------
# FILTER: Keep only "intentional" forks
# NOTE: has_issues_enabled is FALSE by default on all GitHub forks
# so it is NOT a useful filter. Use these instead:
# ---------------------------------------------------------------

# Stage 1: Filter at parse time using fields available in the JSON
# Keep forks where the repo has actual content (size > 0)
# OR the fork owner is an organization (more likely intentional)
intentional_mask = (
    (df["initial_size_kb"] > 0) |
    (df["fork_owner_type"] == "Organization")
)
df_intentional = df[intentional_mask].copy()
df_excluded    = df[~intentional_mask].copy()

print(f"\nTotal forks:       {len(df)}")
print(f"Stage 1 pass:      {len(df_intentional)}  (size > 0 or org owner)")
print(f"Stage 1 excluded:  {len(df_excluded)}")

# ---------------------------------------------------------------
# Stage 2 filter (run AFTER Step 2 BigQuery):
# Keep only forks that had at least 1 commit pushed
# by the fork owner in the first 90 days.
# This is the strongest signal of intentional independent development.
# Add this after merging with BigQuery activity data:
#
#   df_final = df_final[df_final["total_commits"] > 0]
#
# You can also use a stricter threshold like >= 3 commits.
# ---------------------------------------------------------------
print("\nNote: Apply Stage 2 filter after Step 2 BigQuery run:")
print("  Keep repos where total_commits > 0 in early period")

# ---------------------------------------------------------------
# SAVE
# ---------------------------------------------------------------
df_intentional.to_csv(OUTPUT_FILE, index=False)
df_excluded.to_csv("excluded_forks.csv", index=False)

print(f"\nSaved {len(df_intentional)} repos to {OUTPUT_FILE}")
print("\nSample:")
print(df_intentional[["repo_name", "parent_repo_name", "created_at", "fork_owner_type"]].head(10))