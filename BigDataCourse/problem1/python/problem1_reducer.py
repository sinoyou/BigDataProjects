#!/usr/bin/env python

import sys

save = {}

for line in sys.stdin:
    # clear empty space
    line = line.strip()

    doc_id, word = line.split('\t',1)
    if save.has_key(word):
        save[word].append(doc_id)
    else:
        save[word] = []
        save[word].append(doc_id)

for (word, doc_list) in save.items():
    print ([word, doc_list])