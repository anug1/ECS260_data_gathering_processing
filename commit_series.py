import json
import requests
from datetime import datetime, timedelta
from collections import defaultdict
import asyncio
import aiohttp

GITHUB_API_URL = "https://api.github.com/graphql"
GITHUB_TOKEN='your-token'

HEADERS = {
    "Authorization": f"Bearer {GITHUB_TOKEN}"
}

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

async def fetch_commit_dates(session, owner, name, since):
    cursor = None
    dates = []

    while True:
        variables = {
            "owner": owner,
            "name": name,
            "since": since.isoformat() + "Z",
            "cursor": cursor
        }

        async with session.post(
            GITHUB_API_URL,
            json={"query": QUERY, "variables": variables},
            headers=HEADERS,
        ) as resp:
            resp.raise_for_status()
            data = await resp.json()

        # print(data['data'])

        if 'data' not in data:
            print(f"Error: {data}")

        try:
            repo = data["data"]["repository"]
            if repo is None:
                return None

            history = (
                repo["defaultBranchRef"]["target"]["history"]
            )
        except Exception as e:
            print(f"Data: {data}")
            print(f"Error: {e}")
            
            history = None
            break

        if 'edges' in history:
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

async def process_repo(session, repoStr):
    repo = json.loads(repoStr)
    payload = json.loads(repo['payload'])['forkee']
    childOwner, childName = payload['full_name'].split('/')
    # print(repo['repo'].keys())
    parentOwner, parentName = repo['repo']['name'].split('/')
    forkTime = payload['updated_at']
    # print((childOwner, childName, forkTime))
    # since = datetime.utcnow() - timedelta(days=365)
    since = datetime.strptime(forkTime, '%Y-%m-%dT%H:%M:%SZ')
    # print(f"Fetching {childName}...")

    dates = await fetch_commit_dates(session, childOwner, childName, since)
    # print(f'dates: {dates}')
    return {
        'parentName': parentName,
            'parentOwner': parentOwner,
            'childOwner': childOwner,
            'childName': childName,
        'forkTime': forkTime,
        'commitTimes': monthly_timeseries(dates)
    }
    

async def main():

    with open("forked.json") as f:
        repos = f.readlines()

    results = []
    num_threads = 100
    start_repo_i = 16800
    end_repo_i = 19000
    write_filename = "commit_timeseries12.json"
    async with aiohttp.ClientSession() as session:
        for i in range(start_repo_i, end_repo_i, num_threads):
            print(f'starting {i+1}th request')
            end = min(end_repo_i, i + num_threads)
            tasks = [
                process_repo(session, repoStr)
                for repoStr in repos[i:end]

            ]
            results += await asyncio.gather(*tasks, return_exceptions=True)
            print(results)
            with open(write_filename, "w") as f:
                json.dump(results, f, indent=2)
    # print(results)

    with open(write_filename, "w") as f:
        json.dump(results, f, indent=2)


if __name__ == "__main__":
    asyncio.run(main())

