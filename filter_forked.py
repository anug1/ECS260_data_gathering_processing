
#Filter forkeed events and get list of forked events with more than 5 commits in the first 3 months:







import json
from datetime import datetime
from dateutil.relativedelta import relativedelta
import requests
import time

# --- GitHub API setup ---
GITHUB_API_URL = "https://api.github.com/graphql"
GITHUB_TOKEN = ""

HEADERS = {"Authorization": f"Bearer {GITHUB_TOKEN}"}

# --- GraphQL query to get commits ---
QUERY_COMMITS = """
query($owner: String!, $name: String!, $since: GitTimestamp!) {
  repository(owner: $owner, name: $name) {
    defaultBranchRef {
      target {
        ... on Commit {
          history(first: 100, since: $since) {
            totalCount
          }
        }
      }
    }
  }
}
"""

MIN_COMMITS = 15  # Only keep forks with >=15 commits in first 3 months
BATCH_SIZE = 100  # Pause every 100 forks to avoid rate limits

def commits_in_first_3_months(owner, repo, fork_date, retries=3):
    """Count commits in the first 3 months after fork creation"""
    fork_dt = datetime.fromisoformat(fork_date.replace("Z", ""))
    cutoff_dt = fork_dt.replace(day=1) + relativedelta(months=3)

    variables = {"owner": owner, "name": repo, "since": fork_date}

    for attempt in range(retries):
        try:
            resp = requests.post(
                GITHUB_API_URL,
                json={"query": QUERY_COMMITS, "variables": variables},
                headers=HEADERS
            )
            if resp.status_code in [502, 503]:
                time.sleep(2)
                continue
            resp.raise_for_status()
            data = resp.json()
            total_commits = data["data"]["repository"]["defaultBranchRef"]["target"]["history"]["totalCount"]
            return total_commits
        except (requests.RequestException, KeyError, TypeError):
            time.sleep(1)
    return 0  # fallback if all retries fail

# --- Input/output files ---
input_file = "forked.json"
output_file = "filtered_forks_15_commits.jsonl"

with open(input_file, "r") as fin, open(output_file, "w") as fout:
    batch_count = 0
    for line_number, line in enumerate(fin, start=1):
        line = line.strip()
        if not line:
            continue

        event = json.loads(line)
        payload = json.loads(event.get("payload", "{}"))
        forkee = payload.get("forkee", {})

        fork_name = forkee.get("name")
        fork_owner = forkee.get("owner", {}).get("login")
        fork_date = forkee.get("created_at")

        if not fork_name or not fork_owner or not fork_date:
            continue

        commits_count = commits_in_first_3_months(fork_owner, fork_name, fork_date)

        if commits_count >= MIN_COMMITS:
            fout.write(json.dumps(event) + "\n")

        batch_count += 1
        if batch_count >= BATCH_SIZE:
            print(f"Processed {line_number} forks, sleeping 1 sec to avoid rate limits...")
            time.sleep(1)
            batch_count = 0

print(f"Filtered fork events saved to {output_file}")
