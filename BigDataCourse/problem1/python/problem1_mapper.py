#!/usr/bin/env python

import sys
import json

for line in sys.stdin:
    # clear empty space
    line = line.strip()

    record = json.loads(line)
    document_id = record[0]
    text = record[1]

    words = text.split()

    for word in words: 
        # (document_id, text)
        print '%s\t%s' % (word,document_id)