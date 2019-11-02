import sys
import json

for line in sys.stdin:
    line = line.strip()
    record = json.loads(line)

    my_name = record[0]
    friend_name = record[1]

    if my_name > friend_name :
        key = friend_name + '|' + my_name
    else:
        key = my_name + '|' + friend_name

    # !!!: Key must make sure that (name1,name2) & (name2,name1) sent to same reducer.
    print '%s\t%s' % (key, '1')