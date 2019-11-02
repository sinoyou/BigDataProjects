import sys
import json

intermediate = {}
result = []

for line in sys.stdin:
    line = line.strip()
    
    key, value = line.split('\t',1)
    name1, name2 = key.split('|',1)
    
    # add value to intermediate compenent by key:order_id
    intermediate.setdefault(name1,{})
    intermediate[name1].setdefault(name2, 0)
    intermediate[name1][name2] += 1

# for each item in the dictionary 
for (mine, mine_dict) in intermediate.items():
    for (friend, count) in mine_dict.items():
        # friendship is symetric only if count[mine][friend] == 2
        if count == 1:
            result.append((mine, friend))
            result.append((friend, mine))


jenc = json.JSONEncoder()
for item in result:
    print jenc.encode(item)