## FINAL CONSOLIDATE FILE TO GENERATE DATA FOR MONGODB LOAD


import xml.etree.cElementTree as ET
from collections import defaultdict
import re
import pprint
import codecs
import json

# variable import file from working directory
OSMFILE = "adelaide_australia.osm"

# variables for regex patterns
street_type_re = re.compile(r'\b\S+\.?$', re.IGNORECASE)
lower = re.compile(r'^([a-z]|_)*$')
lower_colon = re.compile(r'^([a-z]|_)*:([a-z]|_)*$')
problemchars = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')

# variable updated with additional street types deemed acceptable
expected = ["Street", "Avenue", "Boulevard", "Drive", "Court", "Place", "Square", "Lane", "Road", 
            "Trail", "Parkway", "Commons", "Crescent", "Esplanade", "Grove", "Highway", "Mall",
            "Mews", "Parade", "Terrace", "Walk", "Way"]

# variable updated with street type updates identified during audit
mapping = { "St": "Street",
            "St.": "Street",
            "Ave" : "Avenue",
            "Rd." : "Road",
            "Cresent" : "Crescent",
            "Rd" : "Road",
            "Street23" : "Street",
            "Strert" : "Street",
            "North" : "North"
            }

# compares street types asgainst 'expected' - this is only called during the AUDITING step
def audit_street_type(street_types, street_name):
    m = street_type_re.search(street_name)
    if m:
        street_type = m.group()
        if street_type not in expected:
            street_types[street_type].add(street_name)

            
#return value for key defined as addr:street - this is only called during the AUDITING step
def is_street_name(elem):
    return (elem.attrib['k'] == "addr:street")


# function to activate audit function against street types - this is only called during the AUDITING step
def audit(osmfile):
    osm_file = open(osmfile, "r")
    global street_types
    street_types = defaultdict(set)
    for event, elem in ET.iterparse(osm_file, events=("start",)):

        if elem.tag == "node" or elem.tag == "way":
            for tag in elem.iter("tag"):
                if is_street_name(tag):
                    audit_street_type(street_types, tag.attrib['v'])
    return street_types


# function to update problematic names - this is only called during the AUDITING step
def update_name(name, mapping):
    # name variable replaces problematic names (using regex) with corresponding value from 'mapping' dict
    name = name.replace(street_type_re.search(name).group(),
                        mapping[street_type_re.search(name).group()])

    return name


# function to call AUDITING functions - commented out during MongoDB JSON file creation
'''
def test():
    st_types = audit(OSMFILE)

    for st_type, ways in st_types.iteritems():
        for name in ways:
            better_name = update_name(name, mapping)

'''

########## MongoDB JSON file creation functions ##########


# variable containing key names for top-level 'created' key
CREATED = [ "version", "changeset", "timestamp", "user", "uid"]


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
            elif attrName == "k" and attrValue.startswith("addr:") and street_type_re.search(tag.attrib['v']).group() not in mapping.keys() and attrValue != "addr:postcode":
                subdict[attrValue.replace('addr:','')] = tag.get("v")
            # add 'address' fields to subdict for problematic values - uses regex and 'mapping' dict
            elif attrName == "k" and attrValue.startswith("addr:") and street_type_re.search(tag.attrib['v']).group() in mapping.keys():
                subdict[attrValue.replace('addr:','')] = tag.get("v").replace(street_type_re.search(tag.attrib['v']).group(), mapping[street_type_re.search(tag.attrib['v']).group()])  
            # additional cleaning on 'postal_code' field
            elif attrName == "k" and attrValue == ("postal_code"):
                subdict["postcode"] = tag.get("v")  
            # additional cleaning on column shift affected postcodes eg, 'SA 5082'
            elif attrName == "k" and attrValue == "addr:postcode" and attrValue.isdigit() == False:
                subdict["postcode"] = re.sub("[^0-9]", "", tag.get("v")) 
            # add clean postcodes
            elif attrName == "k" and attrValue == "addr:postcode" and attrValue.isdigit() == True:
                subdict["postcode"] = tag.get("v")
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

    data = process_map(OSMFILE, True)

if __name__ == "__main__":
    test()
