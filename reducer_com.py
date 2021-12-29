#!/usr/bin/env python
"""reducer.py"""

from operator import itemgetter
import sys

current_count = 0
prev_com = None

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # parse the input we got from mapper.py
    # follow point to current node
    com, count = line.split('\t', 1)

     # convert count (currently a string) to int
    try:
        count = int(count)
    except ValueError:
        # count was not a number, so silently
        # ignore/discard this line
        continue

    # this IF-switch only works because Hadoop sorts map output
    # by key (here: word) before it is passed to the reducer
    if com == prev_com:
        current_count += count
    else:
        if prev_com:
            # write result to STDOUT
            print 'Community %s: %s' %(prev_com, current_count)
        # clear
        prev_com = com
        current_count = count

# do not forget to output the last one if needed!
if prev_com:
    print 'Community %s: %s' %(prev_com, current_count)