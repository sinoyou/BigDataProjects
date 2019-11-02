import sys
import json

for line in sys.stdin:
    line = line.strip()
    record = json.loads(line)

    dna_id = record[0]
    dna_seq = record[1]

    dna_seq_deprecated = dna_seq[:-10]

    print '%s\t%s' % (dna_seq_deprecated, dna_id)