"""
Free alternative to BigQuery.
Downloads GH Archive hourly files, filters for your repos, deletes each file after.
Covers Dec 2023 to Jul 2024 (8 months = ~5,800 hourly files).
Runtime estimate: 3-8 hours depending on your internet speed.
Disk needed: < 500MB at any one time.
"""
import requests, gzip, json, os, time
from datetime import datetime, timedelta, timezone
from collections import defaultdict
import pandas as pd

# ---------------------------------------------------------------
# LOAD YOUR REPOS
# ---------------------------------------------------------------
repos_df = pd.read_csv("repos_table.csv", parse_dates=["created_at"])
repos_df["created_at"] = pd.to_datetime(repos_df["created_at"], utc=True)

# Build a lookup dict: repo_name -> fork created_at timestamp
repo_lookup = dict(zip(repos_df["repo_name"], repos_df["created_at"]))
repo_set    = set(repo_lookup.keys())

EARLY_DAYS = 182  # 6 months

print(f"Loaded {len(repo_set)} repos to track.")

# ---------------------------------------------------------------
# ACCUMULATORS — one entry per repo
# ---------------------------------------------------------------
commits       = defaultdict(int)
commit_authors= defaultdict(set)
issues_opened = defaultdict(int)
issues_closed = defaultdict(int)
prs_opened    = defaultdict(int)
prs_merged    = defaultdict(int)
prs_rejected  = defaultdict(int)
releases      = defaultdict(int)
stars         = defaultdict(int)
forks_count   = defaultdict(int)
issue_comments= defaultdict(int)
pr_comments   = defaultdict(int)

# For responsiveness — store (issue_number -> opened_at, first_response_at)
issue_first_open     = defaultdict(dict)  # repo -> {issue_num -> opened_at}
issue_first_response = defaultdict(dict)  # repo -> {issue_num -> first_comment_at}
pr_first_open        = defaultdict(dict)
pr_first_response    = defaultdict(dict)

# For contributor time series
contributor_months = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
# contributor_months[repo][month][contributor] = count

# ---------------------------------------------------------------
# HELPER: check if event is within early period for the repo
# ---------------------------------------------------------------
def in_early_period(repo_name, event_time):
    created = repo_lookup.get(repo_name)
    if created is None:
        return False
    if event_time.tzinfo is None:
        event_time = event_time.replace(tzinfo=timezone.utc)
    return created <= event_time <= created + timedelta(days=EARLY_DAYS)

# ---------------------------------------------------------------
# HELPER: process one event
# ---------------------------------------------------------------
def process_event(event):
    repo_name  = event.get("repo", {}).get("name")
    event_type = event.get("type")
    actor      = event.get("actor", {}).get("login", "")

    if repo_name not in repo_set:
        return

    try:
        event_time = datetime.strptime(
            event["created_at"], "%Y-%m-%dT%H:%M:%SZ"
        ).replace(tzinfo=timezone.utc)
    except Exception:
        return

    if not in_early_period(repo_name, event_time):
        return

    payload = event.get("payload", {})
    month   = event_time.strftime("%Y-%m")

    if event_type == "PushEvent":
        commits[repo_name]        += 1
        commit_authors[repo_name].add(actor)
        contributor_months[repo_name][month][actor] += 1

    elif event_type == "IssuesEvent":
        action = payload.get("action")
        if action == "opened":
            issues_opened[repo_name] += 1
            issue_num = str(payload.get("issue", {}).get("number", ""))
            opened_at_str = payload.get("issue", {}).get("created_at", "")
            if issue_num and opened_at_str:
                try:
                    opened_at = datetime.strptime(opened_at_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
                    if issue_num not in issue_first_open[repo_name]:
                        issue_first_open[repo_name][issue_num] = opened_at
                except Exception:
                    pass
        elif action == "closed":
            issues_closed[repo_name] += 1

    elif event_type == "PullRequestEvent":
        action = payload.get("action")
        merged = payload.get("pull_request", {}).get("merged", False)
        if action == "opened":
            prs_opened[repo_name] += 1
            pr_num = str(payload.get("pull_request", {}).get("number", ""))
            opened_at_str = payload.get("pull_request", {}).get("created_at", "")
            if pr_num and opened_at_str:
                try:
                    opened_at = datetime.strptime(opened_at_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
                    if pr_num not in pr_first_open[repo_name]:
                        pr_first_open[repo_name][pr_num] = opened_at
                except Exception:
                    pass
        elif action == "closed":
            if merged:
                prs_merged[repo_name]   += 1
            else:
                prs_rejected[repo_name] += 1

    elif event_type == "IssueCommentEvent":
        issue_comments[repo_name] += 1
        issue_num = str(payload.get("issue", {}).get("number", ""))
        if issue_num:
            if issue_num not in issue_first_response[repo_name]:
                issue_first_response[repo_name][issue_num] = event_time
            else:
                if event_time < issue_first_response[repo_name][issue_num]:
                    issue_first_response[repo_name][issue_num] = event_time

    elif event_type == "PullRequestReviewCommentEvent":
        pr_comments[repo_name] += 1
        pr_num = str(payload.get("pull_request", {}).get("number", ""))
        if pr_num:
            if pr_num not in pr_first_response[repo_name]:
                pr_first_response[repo_name][pr_num] = event_time
            else:
                if event_time < pr_first_response[repo_name][pr_num]:
                    pr_first_response[repo_name][pr_num] = event_time

    elif event_type == "ReleaseEvent":
        if payload.get("action") == "published":
            releases[repo_name] += 1

    elif event_type == "WatchEvent":
        if payload.get("action") == "started":
            stars[repo_name] += 1

    elif event_type == "ForkEvent":
        forks_count[repo_name] += 1

# ---------------------------------------------------------------
# MAIN LOOP: download and process hourly files
# ---------------------------------------------------------------
start_date = datetime(2023, 12, 1)
end_date   = datetime(2024, 7, 31)

current = start_date
total_hours = int((end_date - start_date).total_seconds() / 3600)
processed   = 0
errors      = 0

print(f"\nProcessing {total_hours} hourly files from {start_date.date()} to {end_date.date()}")
print("This will take several hours. Progress is saved — if it stops, restart and it will skip done files.\n")

PROGRESS_FILE = "progress.txt"
done_files = set()
if os.path.exists(PROGRESS_FILE):
    with open(PROGRESS_FILE) as f:
        done_files = set(f.read().splitlines())
    print(f"Resuming — {len(done_files)} files already processed.")

while current <= end_date:
    for hour in range(24):
        filename = f"{current.strftime('%Y-%m-%d')}-{hour}.json.gz"
        url      = f"https://data.gharchive.org/{filename}"

        if filename in done_files:
            current_hour = current.replace(hour=hour)
            if current_hour > end_date:
                break
            processed += 1
            continue

        try:
            r = requests.get(url, timeout=60)
            if r.status_code == 404:
                # Some hours have no data
                processed += 1
                continue
            r.raise_for_status()

            # Process line by line without saving to disk
            with gzip.open(__import__('io').BytesIO(r.content)) as f:
                for line in f:
                    try:
                        event = json.loads(line)
                        process_event(event)
                    except json.JSONDecodeError:
                        continue

            # Mark as done
            with open(PROGRESS_FILE, "a") as pf:
                pf.write(filename + "\n")

            processed += 1
            if processed % 50 == 0:
                pct = processed / total_hours * 100
                print(f"  Progress: {processed}/{total_hours} files ({pct:.1f}%) | "
                      f"Repos with commits: {len(commits)}")

        except Exception as e:
            errors += 1
            if errors <= 10:
                print(f"  Error on {filename}: {e}")
            time.sleep(2)

    current += timedelta(days=1)

print(f"\nDone processing! {processed} files, {errors} errors.")

# ---------------------------------------------------------------
# BUILD OUTPUT DATAFRAMES
# ---------------------------------------------------------------
print("\nBuilding output files...")

# Activity metrics
activity_rows = []
for repo in repo_set:
    activity_rows.append({
        "repo_name":             repo,
        "total_commits":         commits[repo],
        "unique_commit_authors": len(commit_authors[repo]),
        "issues_opened":         issues_opened[repo],
        "issues_closed":         issues_closed[repo],
        "prs_opened":            prs_opened[repo],
        "prs_merged":            prs_merged[repo],
        "prs_rejected":          prs_rejected[repo],
        "num_releases":          releases[repo],
        "star_count":            stars[repo],
        "fork_count":            forks_count[repo],
        "total_issue_comments":  issue_comments[repo],
        "total_pr_comments":     pr_comments[repo],
    })
df_activity = pd.DataFrame(activity_rows)
df_activity.to_csv("step2b_activity.csv", index=False)
print(f"Saved step2b_activity.csv — {len(df_activity)} repos")
print(f"  Repos with commits: {(df_activity['total_commits'] > 0).sum()}")

# Responsiveness metrics
resp_rows = []
for repo in repo_set:
    # Issue first response times
    issue_response_hours = []
    for issue_num, opened_at in issue_first_open[repo].items():
        if issue_num in issue_first_response[repo]:
            hrs = (issue_first_response[repo][issue_num] - opened_at).total_seconds() / 3600
            if hrs >= 0:
                issue_response_hours.append(hrs)

    # PR first response times
    pr_response_hours = []
    for pr_num, opened_at in pr_first_open[repo].items():
        if pr_num in pr_first_response[repo]:
            hrs = (pr_first_response[repo][pr_num] - opened_at).total_seconds() / 3600
            if hrs >= 0:
                pr_response_hours.append(hrs)

    resp_rows.append({
        "repo_name":                    repo,
        "avg_issue_first_response_hrs": sum(issue_response_hours) / len(issue_response_hours) if issue_response_hours else None,
        "avg_pr_first_response_hrs":    sum(pr_response_hours)    / len(pr_response_hours)    if pr_response_hours    else None,
    })
df_resp = pd.DataFrame(resp_rows)
df_resp.to_csv("step2c_responsiveness.csv", index=False)
print(f"Saved step2c_responsiveness.csv")

# Contributor time series
contrib_rows = []
for repo, months in contributor_months.items():
    for month, contributors in months.items():
        for contributor, count in contributors.items():
            contrib_rows.append({
                "repo_name":    repo,
                "month":        month,
                "contributor":  contributor,
                "commit_count": count,
            })
df_contrib = pd.DataFrame(contrib_rows)
df_contrib.to_csv("step2d_contributors.csv", index=False)
print(f"Saved step2d_contributors.csv — {len(df_contrib)} contributor-month records")

print("\nAll Step 2 outputs saved. Ready for Step 4.")