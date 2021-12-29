#!/usr/bin/env python
"""reducer.py"""

from operator import itemgetter
import sys
import heapq

SIZE = 3

nodes_count = dict()

with open("counts") as f:
    for line in f.readlines():
        line = line.strip()
        node, count = line.split('\t', 1)
        nodes_count[node] = int(count)

head_node = None

tail_node = None

h = []

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
            # update maximum in head_node
            if tail_node:
                sim = float(current_count) / (nodes_count[head_node] + nodes_count[tail_node] - current_count)
                # test
                # print 'Test: %s\t%s\t%s\t%s' %(head_node, tail_node, current_count, current_common)
                element = (sim, tail_node, current_common)
                if len(h) < SIZE:
                    heapq.heappush(h,element)
                else:
                    heapq.heappushpop(h,element)
            # clear
            tail_node = current_tail_node
            current_count = 1
            current_common = [common]
    else:
        # update maximum of head node
        if tail_node:
            sim = float(current_count) / (nodes_count[head_node] + nodes_count[tail_node] - current_count)
            # test
            # print 'Test: %s\t%s\t%s\t%s' %(head_node, tail_node, current_count, current_common)
            element = (sim, tail_node, current_common)
            if len(h) < SIZE:
                heapq.heappush(h,element)
            else:
                heapq.heappushpop(h,element)
        # output result of this head node
        if head_node:
            for e in heapq.nlargest(SIZE,h):
                sim, t, c = e
                print '%s: %s, %s, %s' %(head_node, t, c, sim)
        # clear
        head_node = current_head_node
        tail_node = current_tail_node
        h = []
        current_count = 1
        current_common = [common]

# do not forget to output the last one if needed!
if tail_node:
    sim = float(current_count) / (nodes_count[head_node] + nodes_count[tail_node] - current_count)
    # test
    # print 'Test: %s\t%s\t%s\t%s' %(head_node, tail_node, current_count, current_common)
    element = (sim, tail_node, current_common)
    if len(h) < SIZE:
        heapq.heappush(h,element)
    else:
        heapq.heappushpop(h,element)
    if head_node:
        for e in heapq.nlargest(SIZE,h):
            sim, t, c = e
            print '%s: %s, %s, %s' %(head_node, t, c, sim)