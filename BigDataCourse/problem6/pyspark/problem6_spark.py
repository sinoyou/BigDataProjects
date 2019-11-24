from __future__ import print_function
import json
from pyspark import SparkContext

sc = SparkContext('local', 'test')
textfile = sc.textFile('file:///home/root/BigDataCourse/problem6/matrix.json')

MAXN = 10

# Import as Json Format
read = textfile.map(lambda row: json.loads(row.strip()))
# Map - key = (result_row, result_column, index_of_sum)
map_a = read.filter(lambda x: x[0] == 'a').\
    flatMap(lambda x: [((x[1], i, x[2]), x[3]) for i in range(MAXN+1)])
map_b = read.filter(lambda x: x[0] == 'b').\
        flatMap(lambda x: [((i, x[2], x[1]), x[3]) for i in range(MAXN+1)])
map_union = map_a.union(map_b)

# Reduce
reduce_mul = map_union.groupByKey()\
                      .map(lambda x: (x[0], list(x[1])))\
                      .filter(lambda x: len(x[1]) == 2)\
                      .map(lambda x: (x[0], x[1][0] * x[1][1]))

reduce_sum = reduce_mul.map(lambda x: ((x[0][0], x[0][1]), x[1]))\
                       .reduceByKey(lambda x, y: x + y)\
                       .filter(lambda x: x[1] != 0)

# Generate result as export as Json
write = reduce_sum.map(lambda x: json.dumps((x[0][0], x[0][1], x[1])))
# Print
write.foreach(print)