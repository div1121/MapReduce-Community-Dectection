#!/usr/bin/env python
"""reducer.py"""

from operator import itemgetter
import sys

nodes_label = dict()

with open("label") as f:
    for line in f.readlines():
        line = line.strip()
        node, label = line.split()
        nodes_label[node] = label

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
            if len(same_node_list) >= 2:
                print "%s\t%s" %(nodes_label[prev_node], 1)
            else:
                print "%s\t%s" %(nodes_label[prev_node], 0)
        # clear
        prev_node = current_node
        same_node_list = [follow]

# do not forget to output the last one if needed!
if prev_node:
    if len(same_node_list) >= 2:
        print "%s\t%s" %(nodes_label[prev_node], 1)
    else:
        print "%s\t%s" %(nodes_label[prev_node], 0)