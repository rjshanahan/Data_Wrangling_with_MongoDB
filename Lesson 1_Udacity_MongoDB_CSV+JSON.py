## LESSON 6 Q1: AUDITING - ITERATIVE PARSING/SAX PARSE using ITERPARSE


#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Your task is to use the iterative parsing to process the map file and
find out not only what tags are there, but also how many, to get the
feeling on how much of which data you can expect to have in the map.
The output should be a dictionary with the tag name as the key
and number of times this tag can be encountered in the map as value.

Note that your code will be tested with a different data file than the 'example.osm'
"""
import xml.etree.ElementTree as ET
import pprint

def count_tags(filename):
        # YOUR CODE HERE
    tagdict = {}
    for event, elem in ET.iterparse(filename):
        try:
            if elem.tag in tagdict:
                tagdict[elem.tag] += 1
            else:
                tagdict[elem.tag] = 1
            elem.clear()
        except 'NoneType':
            pass
    return tagdict
        
        

def test():

    tags = count_tags('examples.osm')
    pprint.pprint(tags)
    assert tags == {'bounds': 1,
                     'member': 3,
                     'nd': 4,
                     'node': 20,
                     'osm': 1,
                     'relation': 1,
                     'tag': 7,
                     'way': 1}

    

if __name__ == "__main__":
    test()

