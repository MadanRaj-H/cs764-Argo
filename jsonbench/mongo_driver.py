#!/usr/bin/python
import sys
import json
import time
from pymongo import MongoClient
from collections import OrderedDict
from bson import Code
import subprocess
import pymongo
import Global

mongo_client = MongoClient()
mongo_db = mongo_client.argocompdb
data = mongo_db[Global.collection_name]


def clean_db():
    data = mongo_db[Global.collection_name]
    data.drop()
    mongo_db.joined.drop()


def load_data(input_filename, clean=True):
    if clean:
        clean_db()
    data = mongo_db[Global.collection_name]
    start = time.time()

    load_data = subprocess.Popen(["mongoimport", "--db", "argocompdb", "--collection", Global.collection_name, "--file", input_filename, "--jsonArray"], stdout=subprocess.PIPE)
    load_data.communicate()
    if clean:
        data.create_index([("str1", pymongo.ASCENDING)], unique=False)
        data.create_index([("num", pymongo.ASCENDING)], unique=False)
        data.create_index([("dyn1", pymongo.ASCENDING)], unique=False)
        data.create_index([("nested_arr", pymongo.ASCENDING)], unique=False)

    end = time.time()
    exec_time = round(end - start, 6)
    return exec_time


def process_query(query, params=[]):
    data = mongo_db[Global.collection_name]
    if query == 'query1':
        return data.find({}, {"_id": 0, "str1": 1, "num": 1})

    elif query == 'query2':
        return data.find({}, {"_id":0, "nested_obj.str":1, "nested_obj.num":1})

    elif query == 'query3':
        #query_str = '{"$or" : [ { "sparse_%s0" : {"$exists" : True} },{ "sparse_%s9" : {"$exists" : True} } ] }, {"_id":0, "sparse_%s0":1, "sparse_%s9":1}' % (params[0], params[0], params[0], params[0])
        return data.find(
                    {"$or" : [ { "sparse_"+str(params[0])+"0" : {"$exists" : True} },{ "sparse_"+str(params[0])+"9" : {"$exists" : True} } ] },
                    {"_id":0, "sparse_"+str(params[0])+"0":1, "sparse_"+str(params[0])+"9":1})
        #return data.find(query_str)

    elif query == 'query4':
        return data.find(
                    {"$or" : [ { "sparse_"+str(params[0]) + "0" : {"$exists" : True} },{ "sparse_"+str(params[1])+"0" : {"$exists" : True} } ] },{"_id":0, "sparse_"+str(params[0])+"0":1, "sparse_"+str(params[1])+"0":1})

    elif query == 'query5':
        #return data.find({"str1": "GBRDCMJRGEYTC==="}, {"_id":0})
        return data.find({"str1": params[0]}, {"_id":0})

    elif query == 'query6':
        return data.find({"$and": [{"num": {"$gte": int(params[0])}}, {"num": {"$lte": int(params[1])}}]}, {"_id":0})

    elif query == 'query7':
        return data.find({"$and": [{"dyn1": {"$gte": int(params[0])}}, {"num": {"$lte": int(params[1])}}]}, {"_id":0})
    elif query == 'query8':
        #return data.find({"nested_arr": "the"}, {"_id":0})
        return data.find({"nested_arr": params[0]}, {"_id":0})
    elif query == 'query9':
        #return data.find({"sparse_500": "GBRDCMJQ"}, {"_id":0})
        return data.find({"sparse_"+params[0]: params[1]}, {"_id":0})
    elif query == 'query10':

        res = data.group({"thousandth": True},
                         {"$and": [{"num": {"$gte": int(params[0])}}, {"num": {"$lte": int(params[1])}}]},
                         {"count": 0},
                         """function(obj, prev) {
                               prev.count += 1;
                           }""", )
        # res = data.aggregate([
        #        {"$group" : {"_id":"$thousandth", "count":{"$sum":1}}}
        #        ])
        return res
    elif query == 'query11':
        left_map = Code("""
            function() {
                if(this.num >= %d && this.num <= %d) {
                    var output = {};
		            output["left_id"] = this._id;
	                    output["nested_obj"] = this.nested_obj;
		            output["str1"] = "";
		            output["right_id"] = "";
                    emit(this.nested_obj.str, output);
                }
                var output = {};
		        output["str1"] = this.str1;
		        output["right_id"] = this._id;
		        output["nested_obj"] = "";
		        output["left_id"] = "";
                emit(this.str1, output);
            }
        """ % (int(params[0]), int(params[1])))

        r = Code(""" 
            function(key, values) {
                var result = {};
                values.forEach(function(value) {
                    for(var key in value){
                        if (value.hasOwnProperty(key) && value[key] != "") {
                            result[key] = value[key];
                        }
                    }   
                });

                return result;
            }
        """)

        mongo_db.joined.drop()
        data.map_reduce(left_map, r, {'reduce': 'joined'})
        return mongo_db.joined.find({"$and": [{"value.nested_obj": {"$ne": ""}}, {"value.str1": {"$ne": ""}}]}, {"value.nested_obj.str":1, "_id":0})

def remove_nulls(d):
        dict1 = {}
        for k, v in d.iteritems():
            if k == 'thousandth' or k == 'count':
                 dict1[k] = int(v)
            elif (k == 'nested_arr' and len(v) >  0):
                 dict1[k] = v
            elif k!= 'nested_arr' and v is not None:
                 dict1[k] = v

        return dict1
    #return {k: v for k, v in d.iteritems() if v is not None or (isinstance(v, list) and len(v) > 0)}

def main(argv):

    queries = ['query1', 'query2', 'query3', 'query4', 'query5', 'query6', 'query7', 'query8', 'query9', 'query10'] #, 'query11']

    params = argv
    query_time = ['mongodb']
    for idx, val in enumerate(queries):
        #output_file = 'mongo_query' + str(idx+1) + '.json'
        #fp = open(output_file, 'w')
        start = time.time()
        res = process_query(val, params[idx])
        count = 0
        
        for row in res:
            #res1 = json.loads(json.dumps(row), object_hook=remove_nulls)
            #if idx == 10:
            #    res1 = res1["value"]
            #fp.write(json.dumps(res1, sort_keys=True))
            #fp.write('\n')
            count += 1
        end = time.time()
        exec_time = round(end-start, 6)
        print 'query', (idx+1),'time:', exec_time, 'count:', count
        query_time.append(exec_time)
        #fp.close()
    print query_time
    return query_time 


if __name__ == "__main__":
    main(sys.argv[1:])
