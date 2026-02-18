import json
import re
import glob

# Pattern to match files
pattern = "commit_timeseries*.json"

# Regex to extract the number
regex = re.compile(r"commit_timeseries(\d+)\.json")

# Find all matching files
files = glob.glob(pattern)
print(files)

# Sort files by the number in filename
files.sort(key=lambda f: int(regex.search(f).group(1)))

combined = []

for filename in files:
    with open(filename, "r") as f:
        print(filename)
        data = json.load(f)

        if not isinstance(data, list):
            raise ValueError(f"{filename} does not contain a list")

        combined.extend(data)

# Write combined output
with open("combinedbig.json", "w") as f:
    json.dump(combined, f, indent=2)

print(f"Combined {len(files)} files into combined.json ({len(combined)} total items)")

