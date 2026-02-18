import ijson
import json
import requests
from datetime import datetime, timedelta
from collections import defaultdict
from dateutil.relativedelta import relativedelta



def main():
    # We are filtering entries with more than 5 commits in the first 3 month
    total_commits_in_3m = 5
    filename = "commit_timeseries.json" 

    results = []
    popular_results = []

    with open(filename, 'r') as f:
        items = ijson.items(f, 'item')
        #first_few = [next(items) for _ in range(100)]
        for i in items:
            repo_commits = i["commitTimes"] # dict with month key  and number of commits value
            if repo_commits is not None and len(repo_commits)>0:
                
                results.append(i)
                fork_time_str = i["forkTime"][0:7]
                dt_fork = datetime.strptime(fork_time_str, "%Y-%m")
                dt_1m = dt_fork
                dt_2m = dt_fork + relativedelta(months=1)
                dt_3m = dt_fork + relativedelta(months=2)
                dt_1m_str = str(dt_1m)[0:7] 
                dt_2m_str = str(dt_2m)[0:7]
                dt_3m_str = str(dt_3m)[0:7]

                total_commits_3m = 0
                if dt_1m_str in repo_commits:
                    total_commits_3m += int(repo_commits[dt_1m_str])
                if dt_2m_str in repo_commits:
                    total_commits_3m += int(repo_commits[dt_2m_str])
                if dt_3m_str in repo_commits:
                    total_commits_3m += int(repo_commits[dt_3m_str])
                 
                # if total commits in the first 3 months is more than 5, create new file with just those
                if total_commits_3m > total_commits_in_3m:
                    popular_results.append(i)
                    
    
    with open("repo_good_commits.json", "w") as g:
        json.dump(results, g, indent=2)
    
    with open("repo_healthy.json", "w") as h:
        json.dump(popular_results, h, indent=2)
    
if __name__ == "__main__":
    main()