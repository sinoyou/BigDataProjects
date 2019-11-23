from __future__ import print_function
import json
from pyspark import SparkContext

sc = SparkContext('local', 'test')
textfile = sc.textFile('file:///home/root/BigDataCourse/problem5/dna.json')


# Import as Json Format
read = textfile.map(lambda row: json.loads(row.strip()))

# mapper - (dna[:-10], 1)
map_ = read.map(lambda x: (x[1][:-10], 1))

# reducer - set(dna[:-10])
reduce_ = map_.distinct()

# Export as Json Format
write = reduce_.map(lambda x: json.dumps(x[0]))

# Print
write.foreach(print)
