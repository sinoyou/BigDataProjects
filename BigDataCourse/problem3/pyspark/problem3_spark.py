from __future__ import print_function
import json
from pyspark import SparkContext

sc = SparkContext('local', 'test')
textfile = sc.textFile('file:///home/root/BigDataCourse/problem3/friends.json')

# Import as Json Format
read = textfile.map(lambda row: json.loads(row.strip()))
# mapper - (my_name, f_name)
map_ = read.map(lambda json: (json[0], json[1]))
# reducer - (my_name, [f1_name, f2_name, .., f3_name])
reduce_ = map_.distinct().groupByKey()
# Export as Json Format
write = reduce_.map(lambda out: json.dumps((out[0], len(out[1]))))
# Print
write.foreach(print)
