import json

with open("test.json") as f:
    j = json.load(f)
    print(j)
print(type(j))

print(j)