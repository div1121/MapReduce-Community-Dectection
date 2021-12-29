#!/usr/bin/env python
"""mapper.py"""

import sys

# input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    com, count = line.split('\t')
    # using in degree node as key
    print '%s\t%s' % (com, count)