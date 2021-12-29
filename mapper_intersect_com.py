#!/usr/bin/env python
"""mapper.py"""

import sys

# input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    node_start, node_end = line.split()
    # using in degree node as key
    print '%s\t%s' % (node_start, node_end)