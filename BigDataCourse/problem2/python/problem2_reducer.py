import sys
import json

intermediate = {}
result = []

for line in sys.stdin:
    line = line.strip()
    
    order_id, value = line.split('\t',1)
    value = json.loads(value)
    
    # add value to intermediate compenent by key:order_id
    intermediate.setdefault(order_id,[])
    intermediate[order_id].append(value)

# for each item in the dictionary 
for (order_id, values) in intermediate.items():

    # step 1 : find order record corresponding to order_id (order_id is key in order records)
    for value in values:
        if value[0] == 'order':
            order = value
    
    # step 2 : find line_item type record with same order_id and concat.
    for value in values:
        if value[0] == 'line_item':
            result.append((order+value))


jenc = json.JSONEncoder()
for item in result:
    print jenc.encode(item).strip()