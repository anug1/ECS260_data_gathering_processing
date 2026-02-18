import json
import re
import glob

# Pattern to match files
pattern = "combinedbig.json"
# pattern = "commit_timeseries*.json"

# Regex to extract the number
regex = re.compile(r"commit_timeseries(\d+)\.json")

# Find all matching files
files = glob.glob(pattern)
print(f'files: {files}')

# Sort files by the number in filename
# files.sort(key=lambda f: int(regex.search(f).group(1)))

combined = []

for filename in files:
    # with open('combined.json', "r") as f:
    with open(filename, "r") as f:
        print(filename)
        data = json.load(f)
        print(len(data))
        uniqueSet = set()
        unique = []
        for d in data:
            s = str(d)
            if s not in uniqueSet:
                uniqueSet.add(s)
            else:
                unique.add(s)
        print('unique')
        print(unique)
        print(len(unique))



    # with open(filename, "w") as f:
    # #
        # json.dump(data[:300], f, indent = 2)
        # data = list(data)
        # for i, d in enumerate(data):
        #     if d['parentName'] == 'generative-ai-for-beginners':
        #         print((i, 'microsoft'))
        #         print(data[i])
        #         print(data[i+1])
        #         print(data[i+2])
        #         print(data[i+25])
