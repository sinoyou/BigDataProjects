import sys
import json

for line in sys.stdin:
    line = line.strip()
    record = json.loads(line)

    my_name = record[0]
    friend_name = record[1]

    print '%s\t%s' % (my_name, friend_name)