#!/usr/bin/env python
"""reducer.py"""

from operator import itemgetter
import sys

same_node_list = []
prev_node = None

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # parse the input we got from mapper.py
    # follow point to current node
    current_node, follow = line.split('\t', 1)

    # this IF-switch only works because Hadoop sorts map output
    # by key (here: word) before it is passed to the reducer
    if current_node == prev_node:
        # keep track of all follower
        same_node_list.append(follow)
    else:
        if prev_node:
            # write result to STDOUT
            # u, v pair will have intersection of prev_node
            for u in same_node_list:
                for v in same_node_list:
                    if u != v:
                        key = u + '|' + v
                        print '%s\t%s' % (key,prev_node)
        # clear
        prev_node = current_node
        same_node_list = [follow]

# do not forget to output the last one if needed!
if prev_node:
    for u in same_node_list:
        for v in same_node_list:
            if u != v:
                key = u + '|' + v
                print '%s\t%s' % (key,prev_node)