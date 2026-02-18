import json
import requests
from datetime import datetime, timedelta
from collections import defaultdict
from dateutil.relativedelta import relativedelta
import time

GITHUB_API_URL = "https://api.github.com/graphql"

HEADERS = {
    "Authorization": f"Bearer {GITHUB_TOKEN}"
}


# fetches commit history from a repository’s default branch since a given timestamp
QUERY = """
query($owner: String!, $name: String!, $since: GitTimestamp!, $cursor: String) {
  repository(owner: $owner, name: $name) {
    defaultBranchRef {
      target {
        ... on Commit {
          history(first: 100, since: $since, after: $cursor) {
            pageInfo {
              hasNextPage
              endCursor
            }
            edges {
              node {
                committedDate
              }
            }
          }
        }
      }
    }
  }
}
"""

#Whether issues are enabled, Total open issues, Total closed issues
QUERY2 = """ 
query($owner: String!, $name: String!) {
  repository(owner: $owner, name: $name) {
    hasIssuesEnabled
    issues(states: OPEN) {
      totalCount
    }
    closed: issues(states: CLOSED) {
      totalCount
    }
  }
}
"""
#for stars
QUERY3="""
query($owner: String!, $name: String!) {
  repository(owner: $owner, name: $name) {
    stargazerCount
  }
}
"""

def fetch_commit_dates(owner, name, since):
    cursor = None
    dates = []
    while True:
        variables = {
            "owner": owner,
            "name": name,
            "since": since.isoformat() + "Z",
            "cursor": cursor
        }
        resp = requests.post(
            GITHUB_API_URL,
            json={"query": QUERY, "variables": variables},
            headers=HEADERS,
        )
        resp.raise_for_status()
        data = resp.json()

        if "errors" in data:
            print("GraphQL error:", data["errors"])
            return None

        if "data" not in data or data["data"] is None:
            print("No data returned:", data)
            return None


        repo = data["data"]["repository"]
        if repo is None:
            return None
        history = (
            repo["defaultBranchRef"]["target"]["history"]
        )
        for edge in history["edges"]:
            dates.append(edge["node"]["committedDate"])
        if not history["pageInfo"]["hasNextPage"]:
            break
        cursor = history["pageInfo"]["endCursor"]

    return dates


def monthly_timeseries(commit_dates):
    if commit_dates is None:
        return None
    counts = defaultdict(int)

    for d in commit_dates:
        month = d[:7]  # YYYY-MM
        counts[month] += 1

    return dict(sorted(counts.items()))


def commitsPost2m(commit_dates):
    if not commit_dates:
        return 0
    
    # Convert ISO strings to datetime objects
    dates = sorted(
        datetime.fromisoformat(d.replace("Z", ""))
        for d in commit_dates
    )

    # First commit date
    first_commit = dates[0]

    # Cutoff date = first commit + 2 months
    cutoff_date = first_commit + relativedelta(months=2)

    # Count commits strictly after cutoff
    total = sum(1 for d in dates if d >= cutoff_date)

    return total


def fetch_issues(owner, name, since):
    cursor = None

    while True:
        variables = {
            "owner": owner,
            "name": name,
            "since": since.isoformat() + "Z",
            "cursor": cursor
        }

        resp = requests.post(
            GITHUB_API_URL,
            json={"query": QUERY2, "variables": variables},
            headers=HEADERS,
        )

        resp.raise_for_status()
        data = resp.json()
       
        if "errors" in data:
            error_descr = data["errors"][0]["type"]
            if error_descr == "NOT_FOUND":
                return {"deleted_repo": True}
                
            return "errors"
        else:
            repo1 = data["data"]["repository"]
            
            issues_enabled = repo1["hasIssuesEnabled"]
            open_issues_count = repo1["issues"]["totalCount"]
            closed_issues_count = repo1["closed"]["totalCount"]
            return {
                "issues_enabled": issues_enabled,
                "open_issues_count": open_issues_count,
                "closed_issues_count": closed_issues_count}

def fetch_stars(owner, name):
    variables = {
        "owner": owner,
        "name": name,
    }
    resp = requests.post(
        GITHUB_API_URL,
        json={"query": QUERY3, "variables": variables},
        headers=HEADERS,
    )
    resp.raise_for_status()
    data = resp.json()
    if "errors" in data:
        return None
    stars = data["data"]["repository"]["stargazerCount"]
    return stars

    









def main():
    batch_size = 100

    with open("filtered_forks_15_commits.jsonl", "r", encoding="utf-8") as infile, \
         open("result_final.json", "w", encoding="utf-8") as outfile:

        outfile.write("[\n")
        first_item = True
        batch = []

        for line in infile:
            batch.append(line)

            if len(batch) == batch_size:
                first_item = process_batch(batch, outfile, first_item)
                batch = []
                time.sleep(1)

        # Process remaining lines (if any)
        if batch:
            process_batch(batch, outfile, first_item)

        outfile.write("\n]")



def process_batch(batch, outfile, first_item):
    for repo_str in batch:
        repo = json.loads(repo_str)
        

        
        payload = json.loads(repo['payload'])['forkee']
        childOwner, childName = payload['full_name'].split('/')
        parentOwner, parentName = repo['repo']['name'].split('/')
        forkTime = payload['updated_at']
        since = datetime.strptime(forkTime, '%Y-%m-%dT%H:%M:%SZ')
        dates = fetch_commit_dates(childOwner, childName, since)
        issuesData= fetch_issues(childOwner, childName, since)
        stars = fetch_stars(childOwner, childName)


        result = {
            "parentName": parentName,
                "parentOwner": parentOwner,
                "childOwner": childOwner,
                "childName": childName,
            "forkTime": forkTime,
            "commitTimes": monthly_timeseries(dates),
            "issues": issuesData,
            "commitsPost2m": commitsPost2m(dates),
            "stars": stars
        }

        if not first_item:
            outfile.write(",\n")
        outfile.write(json.dumps(result, indent=2))
        first_item = False

    outfile.flush()
    return first_item



if __name__ == "__main__":
    main()