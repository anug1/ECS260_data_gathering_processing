import requests
import pandas as pd
import time
import os
from datetime import datetime, timedelta

GITHUB_TOKEN  = "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"
HEADERS       = {
    "Authorization": f"Bearer {GITHUB_TOKEN}",
    "Content-Type": "application/json"
}
GRAPHQL_URL   = "https://api.github.com/graphql"
OUTPUT_FILE   = "step3_governance_metadata_nonfork.csv"
PROGRESS_FILE = "step3_progress_nonfork.txt"

# ---------------------------------------------------------------
# RATE LIMIT STATE
# ---------------------------------------------------------------
rate_limit_remaining = 5000
rate_limit_reset_at  = None

def run_graphql(query, variables=None):
    global rate_limit_remaining, rate_limit_reset_at

    if rate_limit_remaining < 50:
        wait = max(int((rate_limit_reset_at - time.time())), 10) if rate_limit_reset_at else 60
        print(f"\n⏳ Rate limit low ({rate_limit_remaining} remaining). Pausing {wait}s...")
        time.sleep(wait)
        rate_limit_remaining = 5000

    payload = {"query": query, "variables": variables or {}}

    for attempt in range(5):
        try:
            r = requests.post(GRAPHQL_URL, json=payload, headers=HEADERS, timeout=30)

            rate_limit_remaining = int(r.headers.get("X-RateLimit-Remaining", rate_limit_remaining))
            reset_ts = r.headers.get("X-RateLimit-Reset")
            if reset_ts:
                rate_limit_reset_at = int(reset_ts)

            if r.status_code == 200:
                data = r.json()
                if "errors" in data:
                    err_msg = data["errors"][0].get("type", "")
                    if err_msg in ("NOT_FOUND", "FORBIDDEN"):
                        return None
                    print(f"  GraphQL error: {data['errors']}")
                    return None
                return data["data"]

            elif r.status_code in (403, 429):
                wait = max(int(rate_limit_reset_at - time.time()), 60) if rate_limit_reset_at else 60
                print(f"\n🛑 Rate limited (HTTP {r.status_code}). Waiting {wait}s...")
                time.sleep(wait)

            elif r.status_code == 502:
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

    return None


# ---------------------------------------------------------------
# QUERIES
# Note: No divergence query needed for non-forks
# since they have no parent to compare against
# ---------------------------------------------------------------
REPO_QUERY = """
query($owner: String!, $name: String!) {
  rateLimit { remaining resetAt }
  repository(owner: $owner, name: $name) {
    nameWithOwner
    isFork
    contributing: object(expression: "HEAD:CONTRIBUTING.md") { id }
    codeOfConduct: object(expression: "HEAD:CODE_OF_CONDUCT.md") { id }
    license: licenseInfo { name }
    cicd: object(expression: "HEAD:.github/workflows") { id }
    primaryLanguage { name }
    repositoryTopics(first: 10) {
      nodes { topic { name } }
    }
  }
}
"""


# ---------------------------------------------------------------
# PROCESS ONE REPO
# Simpler than forks — no parent or divergence queries needed
# So only 1 API call per repo instead of 3
# This means it will run ~3x faster than the fork version!
# ---------------------------------------------------------------
def parse_owner_name(repo_name):
    parts = repo_name.split("/")
    return (parts[0], parts[1]) if len(parts) == 2 else (None, None)

def get_repo_metadata(repo_name):
    global rate_limit_remaining

    owner, name = parse_owner_name(repo_name)
    if not owner:
        return None

    result = {"repo_name": repo_name}

    data = run_graphql(REPO_QUERY, {"owner": owner, "name": name})
    if not data or not data.get("repository"):
        return None

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

    # No parent or divergence fields for non-forks
    result["parent_repo"]              = None
    result["parent_stars"]             = None
    result["parent_forks"]             = None
    result["parent_created_at"]        = None
    result["parent_language"]          = None
    result["early_commits_on_branch"]  = None
    result["parent_early_commits"]     = None
    result["divergence_ratio"]         = None

    return result


# ---------------------------------------------------------------
# MAIN LOOP
# ---------------------------------------------------------------
repos_df   = pd.read_csv("step2b_activity_nonfork.csv")[["repo_name"]].copy()
repos_meta = pd.read_csv("repos_table_nonfork.csv")[["repo_name", "created_at"]]
repos_df   = repos_df.merge(repos_meta, on="repo_name", how="left")

# Load already processed repos for resume
done_repos = set()
if os.path.exists(PROGRESS_FILE):
    with open(PROGRESS_FILE) as f:
        done_repos = set(line.strip() for line in f if line.strip())
    print(f"Resuming — {len(done_repos)} repos already done, "
          f"{len(repos_df) - len(done_repos)} remaining.")

file_exists = os.path.exists(OUTPUT_FILE)
total       = len(repos_df)
batch       = []
BATCH_SIZE  = 50
start_time  = time.time()

for i, row in repos_df.iterrows():
    if row["repo_name"] in done_repos:
        continue

    record = get_repo_metadata(row["repo_name"])
    if record:
        batch.append(record)

    with open(PROGRESS_FILE, "a") as pf:
        pf.write(row["repo_name"] + "\n")
    done_repos.add(row["repo_name"])

    if len(batch) >= BATCH_SIZE:
        df_batch = pd.DataFrame(batch)
        df_batch.to_csv(OUTPUT_FILE, mode="a", header=not file_exists, index=False)
        file_exists = True
        batch = []

    processed = len(done_repos)
    if processed % 100 == 0:
        elapsed   = (time.time() - start_time) / 60
        pct       = processed / total * 100
        remaining = (elapsed / pct * (100 - pct)) if pct > 0 else 0
        print(f"  [{processed}/{total}] {pct:.1f}% | "
              f"Elapsed: {elapsed:.0f}m | "
              f"Est. remaining: {remaining:.0f}m | "
              f"Rate limit: {rate_limit_remaining} pts left")

    time.sleep(0.8)

# Save remaining batch
if batch:
    df_batch = pd.DataFrame(batch)
    df_batch.to_csv(OUTPUT_FILE, mode="a", header=not file_exists, index=False)

print(f"\n✅ Step 3 non-fork complete! Results saved to {OUTPUT_FILE}")