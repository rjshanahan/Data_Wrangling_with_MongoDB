## FIND

#!/usr/bin/env python

import pprint

def get_db(db_name):
    from pymongo import MongoClient
    client = MongoClient('localhost:27017')
    db = client[db_name]
    return db


def doc_query():

    #query = {"address" : {"$exists": 1}}
    #query = {"address.address.postcode" : {"$exists": 1}}
    #query = {"population" : {"$exists": 1}}
    query = {"type" : "water"}

    
    for p in db.adel_coll3.find(query):
        pprint.pprint(p)
    
    return query


def find_doc(db, query):
    return db.adel_coll3.find(query)


if __name__ == "__main__":

    db = get_db("osm_adelaide")
    query = doc_query()
    p = find_doc(db, query)
    
    
    
######################################
### PROJECT SUBMISSION QUERIES

#!/usr/bin/env python

def get_db(db_name):
    from pymongo import MongoClient
    client = MongoClient('localhost:27017')
    db = client[db_name]
    return db

def make_pipeline():
  
    pipeline =          [
                            { "$match" : { "created.user": {"$exists" : 1} } } ,
                            
                            
                            { '$group' : { '_id' : { "user" : "$created.user" }, "counter" : { "$sum" : 1 } } },
                            
                            
                            { '$group' : { '_id' : "$_id.user", 
                                          'number' : { '$sum' : "$counter"} } },
                            
                            
                            #{ '$group' : { '_id' :  "$number" } }, 
                            #{ '$project' : { "_id" : "unique_users", 
                            #              "count": {"$sum" : "$counter"},
                            #              'percentage' : { '$divide': ["$counter",
                            #                                           "$count"] } } },
                            #{ '$project' : {"user" : "$_id.user",
                            #                "number" : "$number",
                            #                "total" : "$_id",
                            #                'share' : { '$divide': ["$number",
                            #                                         "$total" ] } } },
                            { "$sort" : {"number": -1} }
                        ]
        
    return pipeline

def osm_unique_users(db, pipeline):
    result = db.adel_coll3.aggregate(pipeline)
    return result

if __name__ == '__main__':
    db = get_db('osm_adelaide')
    pipeline = make_pipeline()
    result = osm_unique_users(db, pipeline)
    import pprint
    pprint.pprint(result)


####################################################
### PROJECT SUBMISSION QUERIES

#!/usr/bin/env python

def get_db(db_name):
    from pymongo import MongoClient
    client = MongoClient('localhost:27017')
    db = client[db_name]
    return db

def make_pipeline():

    pipeline =          [
                        {"$match": {"created.version" : {"$exists":1},
                                   "created.user" : {"$exists":1},
                                   "source" : {"$exists":1}}}, 
                        {"$group": {"_id":{"user": "$created.user",
                                           "source": "$source",
                                           "version":"$created.version"},
                                   "count":{"$sum":1}}}, 
                        {"$sort":{"_id.version":-1
                                  #"count":-1,
                                  }},
                        {"$limit": 20}

                        ]

        
    return pipeline

def osm_nodes_ways(db, pipeline):
    result = db.adel_coll3.aggregate(pipeline)
    return result

if __name__ == '__main__':
    db = get_db('osm_adelaide')
    pipeline = make_pipeline()
    result = osm_nodes_ways(db, pipeline)
    import pprint
    pprint.pprint(result)


#####################################################

### PROJECT SUBMISSION QUERIES

#!/usr/bin/env python

def get_db(db_name):
    from pymongo import MongoClient
    client = MongoClient('localhost:27017')
    db = client[db_name]
    return db

def make_pipeline():
    
    

    #pipeline =          [
    #                    {"$match":{"postal_code":{"$exists":1}}}, 
    #                    {"$group":{"_id":"$postal_code", "count":{"$sum":1}}}, 
    #                    {"$sort":{"count":-1}}
    #                    ]
    
    
    pipeline =          [
                        {"$match":{"address.city":{"$exists":1},
                                   "address.housenumber":{"$exists":1},
                                   "address.street":{"$exists":1},
                                   "address.postcode":{"$exists":1} } }, 
                        {"$group":{"_id":"_id", "count":{"$sum":1}}}, 
                        {"$sort":{"count":-1}}
                        ] 

    
    return pipeline

def osm_node_types(db, pipeline):
    result = db.adel_coll3.aggregate(pipeline)
    return result

if __name__ == '__main__':
    db = get_db('osm_adelaide')
    pipeline = make_pipeline()
    result = osm_node_types(db, pipeline)
    import pprint
    pprint.pprint(result)


########################################
### PROJECT SUBMISSION QUERIES

#!/usr/bin/env python

def get_db(db_name):
    from pymongo import MongoClient
    client = MongoClient('localhost:27017')
    db = client[db_name]
    return db

def make_pipeline():
    
    
    #qty: { "$nin" : [ "id","type","visible","created","pos","addrress" ] 

    pipeline =          [

                        
                        {"$match": {"created.user" : {"$exists":1}}}, 
                        {"$group": {"_id":{"user": "$created.user"},
                                   "count":{"$sum":1}}}, 
                        {"$sort":{"count":-1}},
                        {"$limit": 5}
                        
                        ]
    
    
                        #{"$match": {"address.postcode" : {"$exists":1},
                        #           "created.user" : {"$exists":1}}}, 
                        #{"$group": {"_id":{"postcode":"$address.postcode",
                        #                  "user": "$created.user"
                        #                  },
                        #           "count":{"$sum":1}}}, 
                        #{"$sort":{"count":-1}},
                        #{"$limit": 10}
    
    
                        
                        #{"$match":{"address.postcode" : {"$exists":1},
                        #           "created.user" : {"$exists":1}}}, 
                        #{"$group":{"_id":"$address.postcode", "count":{"$sum":1}}}, 
                        #{"$sort":{"count":-1}}
                        

    
    return pipeline

def osm_node_types(db, pipeline):
    result = db.adel_coll3.aggregate(pipeline)
    return result

if __name__ == '__main__':
    db = get_db('osm_adelaide')
    pipeline = make_pipeline()
    result = osm_node_types(db, pipeline)
    import pprint
    pprint.pprint(result)


#################################

### PROJECT SUBMISSION QUERIES

#!/usr/bin/env python

def get_db(db_name):
    from pymongo import MongoClient
    client = MongoClient('localhost:27017')
    db = client[db_name]
    return db

def make_pipeline():
    
    
    #qty: { "$nin" : [ "id","type","visible","created","pos","addrress" ] 

    pipeline =          [
                        {"$match":{"population":{"$exists":1}}}, 
                        #{"$group":{"_id":"$population", "count":{"$sum":"$population"}}}, 
                        {"$group":{"_id":"$population", "count":{"$sum":1}}}, 
                        {"$sort":{"count":-1}}
                        ]
    

    
    return pipeline

def osm_node_types(db, pipeline):
    result = db.adel_coll3.aggregate(pipeline)
    return result

if __name__ == '__main__':
    db = get_db('osm_adelaide')
    pipeline = make_pipeline()
    result = osm_node_types(db, pipeline)
    import pprint
    pprint.pprint(result)

