from __future__ import print_function
import json
from pyspark import SparkContext

sc = SparkContext('local', 'test')
textfile = sc.textFile('file:///home/root/BigDataCourse/problem4/friends.json')


def get_key(json):
    return (json[0],json[1]) if json[0] > json[1] else (json[1], json[0])

# note
# In problem 4, we need to create new key to ensure reducer get right answer when getting tuples with same key.

# Import as Json Format
read = textfile.map(lambda row: json.loads(row.strip()))

# mapper - ((namex, namey), 1)
map_ = read.map(lambda x: (get_key(x), 1))

# reducer - ((namex, namey), cnt == 1)
reduce_ = map_.reduceByKey(lambda a,b: a+b).filter(lambda x: x[1] == 1)

# Export as Json Format - (namex, namey) & (namey, namex)
write = reduce_.map(lambda x: x[0])
write = write.flatMap(lambda x: [json.dumps((x[0],x[1])), json.dumps((x[1],x[0]))]).sortBy(lambda x:x)

# Print
write.foreach(print)
