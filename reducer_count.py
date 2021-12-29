#!/usr/bin/env python
"""reducer.py"""

from operator import itemgetter
import sys

head_node = None

tail_node = None

max_tail = None
max_count = 0
max_common = []

current_common = []
current_count = 0

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # parse the input we got from mapper.py
    key, common = line.split('\t', 1)

    # head, tail, common
    current_head_node, current_tail_node = key.split('|', 1)

    # this IF-switch only works because Hadoop sorts map output
    # by key (here: word) before it is passed to the reducer
    if head_node == current_head_node:
        # adding counter
        if tail_node == current_tail_node:
            current_count += 1
            current_common.append(common)
        else:
            # test
            # print 'Test: %s\t%s\t%s\t%s' %(head_node, tail_node, current_count, current_common)
            # update maximum in head_node
            if tail_node:
                if max_count < current_count:
                    max_count = current_count
                    max_common = current_common
                    max_tail = tail_node
            # clear
            tail_node = current_tail_node
            current_count = 1
            current_common = [common]
    else:
        # test
        # print 'Test: %s\t%s\t%s\t%s' %(head_node, tail_node, current_count, current_common)
        # update maximum of head node
        if tail_node:
            if max_count < current_count:
                max_count = current_count
                max_common = current_common
                max_tail = tail_node
        # output result of this head node
        if head_node:
            print '%s\t%s\t%s\t%s' %(head_node, max_tail, max_count, max_common)
        # clear
        head_node = current_head_node
        tail_node = current_tail_node
        max_tail = None
        max_count = 0
        max_common = []
        current_count = 1
        current_common = [common]

# do not forget to output the last one if needed!
if tail_node:
    if max_count < current_count:
        max_count = current_count
        max_common = current_common
        max_tail = tail_node
    if head_node:
        print '%s\t%s\t%s\t%s' %(head_node, max_tail, max_count, max_common)