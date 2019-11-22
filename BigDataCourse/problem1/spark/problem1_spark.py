from __future__ import print_function
import json
from pyspark import SparkContext

sc = SparkContext('local', 'test')
textfile = sc.textFile('file:///home/root/BigDataCourse/problem1/books.json')

# Import as Json Format
read = textfile.map(lambda row: json.loads(row.strip()))
# mapper
map_ = read.flatMap(lambda json: [(x, json[0]) for x in json[1].split()])
# reducer
reduce_ = map_.distinct().groupByKey()
# Export as Json Format
write = reduce_.map(lambda out: json.dumps((out[0], list(out[1]))))
# Print
write.foreach(print)
