import sys
import json

MAXN = 10

for line in sys.stdin:
    line = line.strip()
    record = json.loads(line)

    matrix, row, column, value = record[0], record[1], record[2], record[3]
    if matrix == 'a':
        for i in range(MAXN+1):
            k = (row, i)
            v = record
            print '%s\t%s' % (json.dumps(k), json.dumps(v))
    elif matrix == 'b':
        for i in range(MAXN+1):
            k = (i, column)
            v = record
            print '%s\t%s' % (json.dumps(k), json.dumps(v))