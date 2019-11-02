import sys
import json

intermediate = {}

for line in sys.stdin:
    line = line.strip()
    
    key, value = line.split('\t',1)
    
    # add value to intermediate compenent by key:order_id
    intermediate.setdefault(key,[])
    intermediate[key].append(value)

# for each item in the dictionary 
jenc = json.JSONEncoder()
for (key,value) in intermediate.items():
    print jenc.encode(key)