## LESSON 6 Q5: PROCESSING

#!/usr/bin/env python
# -*- coding: utf-8 -*-
import xml.etree.ElementTree as ET
import pprint
import re
import codecs
import json



# REQUIRES:
# street_type_re regex
# mapping variable


CREATED = [ "version", "changeset", "timestamp", "user", "uid"]


mapping = { "St": "Street",
            "St.": "Street",
            "Ave" : "Avenue",
            "Rd." : "Road"
            }

lower = re.compile(r'^([a-z]|_)*$')
lower_colon = re.compile(r'^([a-z]|_)*:([a-z]|_)*$')
problemchars = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')
street_type_re = re.compile(r'\b\S+\.?$', re.IGNORECASE)


# function to create float array of latitude and longitude
def pos_array(element, attrName, attrValue):
    posarray = []
    for attrName, attrValue in element.attrib.items():
        if attrName == "lat"or attrName == "lon":
            posarray.append(float(attrValue))
        else:
            pass
    return posarray


# function to create sub-dictionary for top-level key 'created'
def creator(element, attrName, attrValue):
    subdict = {}
    for attrName, attrValue in element.attrib.items():
        if attrName == "version":
            subdict["version"] = attrValue
        elif attrName == "changeset":
            subdict["changeset"] = attrValue
        elif attrName == "timestamp":
            subdict["timestamp"] = attrValue
        elif attrName == "user":
            subdict["user"] = attrValue
        else:
            if attrName == "uid":
                subdict["uid"] = attrValue
    return subdict


# function to create sub-dictionary for top-level key 'address'
def addressor(element):
    subdict = {}
    # variable to define acceptable keys for subdict
    notaddress = ["postcode","housenumber","housename","street","city","country","state"]
    for tag in element.iter():
        for attrName, attrValue in tag.attrib.items():
            
            # ignores value if it contains bad characters from regex variable 'problemchars'
            if problemchars.search(attrValue) is not None:
                pass
            # ignores values if they contain multiple colons
            elif attrValue.count(':') > 1:
                pass
            # add 'address' fields to subdict for non problematic values
            elif attrName == "k" and attrValue.startswith("addr:") and street_type_re.search(tag.attrib['v']).group() not in mapping.keys():
                subdict[attrValue.replace('addr:','')] = tag.get("v")
            # add 'address' fields to subdict for problematic values - uses regex and 'mapping' dict
            elif attrName == "k" and attrValue.startswith("addr:") and street_type_re.search(tag.attrib['v']).group() in mapping.keys():
                subdict[attrValue.replace('addr:','')] = tag.get("v").replace(street_type_re.search(tag.attrib['v']).group(), mapping[street_type_re.search(tag.attrib['v']).group()])  
            else:
                pass
    # do not return empty dicts
    return subdict if len(subdict) > 0 else None


# function to add other tag/node 'k' values to to main dictionary eg, amenity
def amenity(element):
    for tag in element.iter():
        for attrName, attrValue in tag.attrib.items():
            if problemchars.search(attrValue) is not None:
                pass
            elif attrValue.count(':') > 1:
                pass
            elif attrName == "k"and not attrValue.startswith("addr:"):
                node[attrValue] = tag.get("v")
            else:
                pass
    return node


# function to create array of node references
def noder(element):
    nrefs = []
    for tag in element.iter("nd"):
        for attrName, attrValue in tag.attrib.items():
            if attrName == "ref":
                nrefs.append(tag.get("ref"))
    # do not return empty lists            
    return nrefs if len(nrefs) > 0 else None


# main function to create dictionary for MongoDB import
def shape_element(element):
    # 'node' global as it's called in 'noder' function
    global node

    node = {}

    if element.tag == "node" or element.tag == "way":
        node["type"] = element.tag
        
        amenity(element)
        
        if addressor(element) is None:
            pass
        else:
            node["address"] = addressor(element)
        if noder(element) is None:
            pass
        else:
            node["node_refs"] = noder(element)
        
        for attrName, attrValue in element.attrib.items():
            if attrName == "id":
                node["id"] = attrValue
            elif attrName == "visible":
                node["visible"] = attrValue
            elif attrName == "lat"or attrName == "lon":
                node["pos"] = pos_array(element, attrName, attrValue)
            elif attrName in CREATED:
                node["created"] = creator(element, attrName, attrValue)

        return node
    else:
        return None


# function to write constructed list of dicts to file - MongoDB import ready
def process_map(file_in, pretty = False):
    file_out = "{0}.json".format(file_in)
    data = []
    with codecs.open(file_out, "w") as fo:
        for _, element in ET.iterparse(file_in):
            el = shape_element(element)
            if el:
                data.append(el)
                if pretty:
                    fo.write(json.dumps(el, indent=2)+"\n")
                else:
                    fo.write(json.dumps(el) + "\n")
    return data


def test():
    global data
    data = process_map('example.osm', True)
    pprint.pprint(data)

    assert data[0] == {
                        "id": "261114295", 
                        "visible": "true", 
                        "type": "node", 
                        "pos": [
                          41.9730791, 
                          -87.6866303
                        ], 
                        "created": {
                          "changeset": "11129782", 
                          "user": "bbmiller", 
                          "version": "7", 
                          "uid": "451048", 
                          "timestamp": "2012-03-28T18:31:23Z"
                        }
                      }
    assert data[-1]["address"] == {
                                    "street": "West Lexington St.", 
                                    "housenumber": "1412"
                                      }
    assert data[-1]["node_refs"] == [ "2199822281", "2199822390",  "2199822392", "2199822369", 
                                    "2199822370", "2199822284", "2199822281"]

if __name__ == "__main__":
    test()
