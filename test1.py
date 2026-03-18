from google.cloud import bigquery

client = bigquery.Client(project="oss-sustainability-study")

# Test: count your repos table
query = """
SELECT COUNT(*) as total, 
       COUNTIF(fork_owner_type = 'Organization') as org_forks,
       COUNTIF(fork_owner_type = 'User') as user_forks
FROM `oss-sustainability-study.fork_study.repos`
"""
df = client.query(query).to_dataframe()
print(df)