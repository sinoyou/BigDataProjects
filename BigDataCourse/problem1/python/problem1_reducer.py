#!/usr/bin/env python

import sys
import json

save = {}

for line in sys.stdin:
    # clear empty space
    line = line.strip()

    word, doc_id = line.split('\t',1)
    if save.has_key(word):
        if not doc_id in save[word]:
            save[word].append(doc_id)
    else:
        save[word] = []
        if not doc_id in save[word]:
            save[word].append(doc_id)

result = []

for (word, doc_list) in save.items():
    result.append((word, doc_list))

    
jenc = json.JSONEncoder()
for item in result:
    print jenc.encode(item)