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

# process each element in the final result matrix
for (k ,values) in intermediate.items():
    sum_ = 0
    temp = {}
    for value in values:
        record = json.loads(value)
        if record[0] == 'a':
            if temp.has_key(record[2]):
                sum_ += temp[record[2]] * int(record[3])
            else:
                temp[record[2]] = int(record[3])
        elif record[0] == 'b':
            if temp.has_key(record[1]):
                sum_ += temp[record[1]] * int(record[3])
            else:
                temp[record[1]] = int(record[3])
    
    # form result
    k_json = json.loads(k)
    result_i = k_json[0]
    result_j = k_json[1]
    if sum_ != 0:
        result.append((result_i, result_j, sum_))

# for each item in the dictionary 
jenc = json.JSONEncoder()
for r in result:
    print jenc.encode(r)