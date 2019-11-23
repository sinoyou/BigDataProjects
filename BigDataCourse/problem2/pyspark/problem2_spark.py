from __future__ import print_function
import json
from pyspark import SparkContext

sc = SparkContext('local', 'test')
textfile = sc.textFile('file:///home/root/BigDataCourse/problem2/records.json')

def generate(x):
    order_id, values = x[0], x[1]
    result = []
    for value in list(values):
        if value[0] == 'order':
            order = value
    for value in list(values):
        if value[0] == 'line_item':
            result.append((order+value))
    return result

# Import as Json Format
read = textfile.map(lambda row: json.loads(row.strip()))
# mappe - (order_id, value)
map_ = read.map(lambda x: (x[1], x))
# reduce
reduce_ = map_.groupByKey().map(generate)     # (order_id, [value1, value2, ...])
# Export as Json Format
write = reduce_.flatMap(lambda x: x)               
write = write.map(lambda out: json.dumps(out))
# Print
write.foreach(print)
