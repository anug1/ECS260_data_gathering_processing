import requests
import pandas as pd
import time
import os
from datetime import datetime, timedelta

GITHUB_TOKEN = "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"
HEADERS = {
    "Authorization": f"Bearer {GITHUB_TOKEN}",
    "Content-Type": "application/json"
}
GRAPHQL_URL   = "https://api.github.com/graphql"
OUTPUT_FILE   = "step3_governance_metadata.csv"
PROGRESS_FILE = "step3_progress.txt"

# ---------------------------------------------------------------
# RATE LIMIT STATE
# GitHub GraphQL allows 5000 points/hour
# Each query costs ~1 point. We make up to 3 calls per repo.
# We proactively pause when remaining points get low.
# ---------------------------------------------------------------
rate_limit_remaining = 5000
rate_limit_reset_at  = None

def run_graphql(query, variables=None):
    global rate_limit_remaining, rate_limit_reset_at

    # Proactively pause if running low on points
    if rate_limit_remaining < 50:
        if rate_limit_reset_at:
            wait = max(int((rate_limit_reset_at - time.time())), 10)
        else:
            wait = 60
        print(f"\n⏳ Rate limit low ({rate_limit_remaining} remaining). Pausing {wait}s...")
        time.sleep(wait)
        rate_limit_remaining = 5000  # reset estimate after waiting

    payload = {"query": query, "variables": variables or {}}

    for attempt in range(5):
        try:
            r = requests.post(GRAPHQL_URL, json=payload, headers=HEADERS, timeout=30)

            # Always update rate limit state from headers
            rate_limit_remaining = int(r.headers.get("X-RateLimit-Remaining", rate_limit_remaining))
            reset_ts = r.headers.get("X-RateLimit-Reset")
            if reset_ts:
                rate_limit_reset_at = int(reset_ts)

            if r.status_code == 200:
                data = r.json()
                if "errors" in data:
                    # Repo not found or access denied — skip silently
                    err_msg = data["errors"][0].get("type", "")
                    if err_msg in ("NOT_FOUND", "FORBIDDEN"):
                        return None
                    print(f"  GraphQL error: {data['errors']}")
                    return None
                return data["data"]

            elif r.status_code in (403, 429):
                # Hard rate limit hit — wait until reset
                wait = max(int(rate_limit_reset_at - time.time()), 60) if rate_limit_reset_at else 60
                print(f"\n🛑 Rate limited (HTTP {r.status_code}). Waiting {wait}s...")
                time.sleep(wait)

            elif r.status_code == 502:
                # GitHub temporary error — short wait and retry
                time.sleep(10 * (attempt + 1))

            else:
                print(f"  HTTP {r.status_code} on attempt {attempt+1}")
                time.sleep(5)

        except requests.exceptions.Timeout:
            print(f"  Timeout on attempt {attempt+1}, retrying...")
            time.sleep(10)
        except requests.exceptions.ConnectionError:
            print(f"  Connection error on attempt {attempt+1}, retrying...")
            time.sleep(15)

    return None  # all attempts failed


# ---------------------------------------------------------------
# QUERIES
# ---------------------------------------------------------------
REPO_QUERY = """
query($owner: String!, $name: String!) {
  rateLimit { remaining resetAt }
  repository(owner: $owner, name: $name) {
    nameWithOwner
    isFork
    parent {
      nameWithOwner
      stargazerCount
      forkCount
      createdAt
      primaryLanguage { name }
    }
    contributing: object(expression: "HEAD:CONTRIBUTING.md") { id }
    codeOfConduct: object(expression: "HEAD:CODE_OF_CONDUCT.md") { id }
    license: licenseInfo { name }
    cicd: object(expression: "HEAD:.github/workflows") { id }
    primaryLanguage { name }
    repositoryTopics(first: 10) {
      nodes { topic { name } }
    }
    defaultBranchRef { name }
  }
}
"""

DIVERGENCE_QUERY = """
query($owner: String!, $name: String!, $since: GitTimestamp!, $until: GitTimestamp!) {
  rateLimit { remaining resetAt }
  repository(owner: $owner, name: $name) {
    defaultBranchRef {
      target {
        ... on Commit {
          history(since: $since, until: $until) { totalCount }
        }
      }
    }
  }
}
"""


# ---------------------------------------------------------------
# PROCESS ONE REPO
# ---------------------------------------------------------------
def parse_owner_name(repo_name):
    parts = repo_name.split("/")
    return (parts[0], parts[1]) if len(parts) == 2 else (None, None)

def get_repo_metadata(repo_name, created_at):
    global rate_limit_remaining

    owner, name = parse_owner_name(repo_name)
    if not owner:
        return None

    result = {"repo_name": repo_name}

    # --- Call 1: governance + metadata ---
    data = run_graphql(REPO_QUERY, {"owner": owner, "name": name})
    if not data or not data.get("repository"):
        return None

    # Update rate limit from response body (more accurate than headers)
    if data.get("rateLimit"):
        rate_limit_remaining = data["rateLimit"]["remaining"]

    repo = data["repository"]
    result["has_contributing"]    = repo["contributing"] is not None
    result["has_code_of_conduct"] = repo["codeOfConduct"] is not None
    result["has_license"]         = repo["license"] is not None
    result["has_cicd"]            = repo["cicd"] is not None
    result["primary_language"]    = repo["primaryLanguage"]["name"] if repo["primaryLanguage"] else None
    result["topics"]              = ",".join(
        [t["topic"]["name"] for t in repo["repositoryTopics"]["nodes"]]
    )

    parent = repo.get("parent")
    if parent:
        result["parent_repo"]       = parent["nameWithOwner"]
        result["parent_stars"]      = parent["stargazerCount"]
        result["parent_forks"]      = parent["forkCount"]
        result["parent_created_at"] = parent["createdAt"]
        result["parent_language"]   = parent["primaryLanguage"]["name"] if parent["primaryLanguage"] else None
    else:
        result["parent_repo"]       = None
        result["parent_stars"]      = None
        result["parent_forks"]      = None
        result["parent_created_at"] = None
        result["parent_language"]   = None

    # --- Call 2: fork divergence ---
    if isinstance(created_at, str):
        created_at = datetime.fromisoformat(created_at.replace("Z", "+00:00"))

    since = created_at.isoformat()
    until = (created_at + timedelta(days=182)).isoformat()

    div_data = run_graphql(DIVERGENCE_QUERY, {
        "owner": owner, "name": name,
        "since": since, "until": until
    })

    if div_data:
        if div_data.get("rateLimit"):
            rate_limit_remaining = div_data["rateLimit"]["remaining"]
        ref = div_data.get("repository", {}).get("defaultBranchRef")
        if ref:
            result["early_commits_on_branch"] = ref["target"]["history"]["totalCount"]
        else:
            result["early_commits_on_branch"] = None
    else:
        result["early_commits_on_branch"] = None

    # --- Call 3: parent divergence (only if fork has a parent) ---
    if parent:
        p_owner, p_name = parse_owner_name(parent["nameWithOwner"])
        p_data = run_graphql(DIVERGENCE_QUERY, {
            "owner": p_owner, "name": p_name,
            "since": since, "until": until
        })
        if p_data:
            if p_data.get("rateLimit"):
                rate_limit_remaining = p_data["rateLimit"]["remaining"]
            ref = p_data.get("repository", {}).get("defaultBranchRef")
            if ref:
                p_commits = ref["target"]["history"]["totalCount"]
                result["parent_early_commits"] = p_commits
                total = (result["early_commits_on_branch"] or 0) + p_commits
                result["divergence_ratio"] = (
                    result["early_commits_on_branch"] / total if total > 0 and result["early_commits_on_branch"] is not None else None
                )
            else:
                result["parent_early_commits"] = None
                result["divergence_ratio"]     = None
        else:
            result["parent_early_commits"] = None
            result["divergence_ratio"]     = None
    else:
        result["parent_early_commits"] = None
        result["divergence_ratio"]     = None

    return result


# ---------------------------------------------------------------
# MAIN LOOP
# ---------------------------------------------------------------
repos_df   = pd.read_csv("step2b_activity.csv")[["repo_name"]].copy()
repos_meta = pd.read_csv("repos_table.csv")[["repo_name", "created_at"]]
repos_df   = repos_df.merge(repos_meta, on="repo_name", how="left")

# Load already processed repos for resume
done_repos = set()
if os.path.exists(PROGRESS_FILE):
    with open(PROGRESS_FILE) as f:
        done_repos = set(line.strip() for line in f if line.strip())
    print(f"Resuming — {len(done_repos)} repos already done, "
          f"{len(repos_df) - len(done_repos)} remaining.")

# Check if output file exists (for append mode)
file_exists = os.path.exists(OUTPUT_FILE)

total      = len(repos_df)
batch      = []
BATCH_SIZE = 50   # save to CSV every 50 repos
start_time = time.time()

for i, row in repos_df.iterrows():
    if row["repo_name"] in done_repos:
        continue

    record = get_repo_metadata(row["repo_name"], row["created_at"])
    if record:
        batch.append(record)

    # Mark progress
    with open(PROGRESS_FILE, "a") as pf:
        pf.write(row["repo_name"] + "\n")
    done_repos.add(row["repo_name"])

    # Save batch to CSV
    if len(batch) >= BATCH_SIZE:
        df_batch = pd.DataFrame(batch)
        df_batch.to_csv(OUTPUT_FILE, mode="a", header=not file_exists, index=False)
        file_exists = True
        batch = []

    # Progress report every 100 repos
    processed = len(done_repos)
    if processed % 100 == 0:
        elapsed  = (time.time() - start_time) / 60
        pct      = processed / total * 100
        remaining = (elapsed / pct * (100 - pct)) if pct > 0 else 0
        print(f"  [{processed}/{total}] {pct:.1f}% | "
              f"Elapsed: {elapsed:.0f}m | "
              f"Est. remaining: {remaining:.0f}m | "
              f"Rate limit: {rate_limit_remaining} pts left")

    # Polite delay between repos — keeps us well under rate limit
    time.sleep(0.8)

# Save any remaining batch
if batch:
    df_batch = pd.DataFrame(batch)
    df_batch.to_csv(OUTPUT_FILE, mode="a", header=not file_exists, index=False)

print(f"\n✅ Step 3 complete! Results saved to {OUTPUT_FILE}")