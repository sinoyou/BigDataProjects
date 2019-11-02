import sys
import json

intermediate = {}
result = []

for line in sys.stdin:
    line = line.strip()
    
    key, value = line.split('\t',1)
    
    # add value to intermediate compenent by key:order_id
    intermediate.setdefault(key,[])
    intermediate[key].append(value)

# for each item in the dictionary 
for (key, value) in intermediate.items():
    result.append((key,len(value)))


jenc = json.JSONEncoder()
for item in result:
    print jenc.encode(item)