#!/usr/bin/python
import json, sys
import time
import csv
import string
import Global

sys.path.append("../pyqs")

import pyqs as qs

def clean_db():
    try:
        qs.query("drop table {};".format(Global.argo_str_tbl))
        qs.query("drop table {};".format(Global.argo_num_tbl))
        qs.query("drop table {};".format(Global.argo_bool_tbl))
    except:
        print "Table exists"

    qs.query("create table {} (objid bigint, keystr varchar(100), valstr varchar(100)) WITH BLOCKPROPERTIES (TYPE compressed_columnstore, SORT objid, COMPRESS ALL, BLOCKSIZEMB 8);".format(Global.argo_str_tbl))
    qs.query("create table {} (objid bigint, keystr varchar(100), valnum double) WITH BLOCKPROPERTIES (TYPE compressed_columnstore, SORT objid, COMPRESS ALL, BLOCKSIZEMB 8);".format(Global.argo_num_tbl))
    qs.query("create table {} (objid bigint, keystr varchar(100), valbool integer) WITH BLOCKPROPERTIES (TYPE compressed_columnstore, SORT objid, COMPRESS ALL, BLOCKSIZEMB 8);".format(Global.argo_bool_tbl))

    '''
    # split_rowstore
    qs.query("create table {} (objid bigint, keystr varchar(100), valstr varchar(100)) WITH BLOCKPROPERTIES (TYPE split_rowstore, BLOCKSIZEMB 8);".format(Global.argo_str_tbl))
    qs.query("create table {} (objid bigint, keystr varchar(100), valnum double) WITH BLOCKPROPERTIES (TYPE split_rowstore, BLOCKSIZEMB 8);".format(Global.argo_num_tbl))
    qs.query("create table {} (objid bigint, keystr varchar(100), valbool integer) WITH BLOCKPROPERTIES (TYPE split_rowstore, BLOCKSIZEMB 8);".format(Global.argo_bool_tbl))
    '''

    #indices
    #qs.query("CREATE INDEX argo_nobench_main_idx_str_objid_sma ON argo_str_tbl (objid) using sma;".replace("argo_str_tbl", Global.argo_str_tbl))
    #qs.query("CREATE INDEX argo_nobench_main_idx_num_objid_sma ON argo_num_tbl (objid) using sma;".replace("argo_num_tbl", Global.argo_num_tbl))
    #qs.query("CREATE INDEX argo_nobench_main_idx_bool_objid_sma ON argo_bool_tbl (objid) using sma;".replace("argo_bool_tbl", Global.argo_bool_tbl))
    #qs.query("CREATE INDEX argo_nobench_main_idx_str_objid ON argo_str_tbl (objid) using csbtree;".replace("argo_str_tbl", Global.argo_str_tbl))
    #qs.query("CREATE INDEX argo_nobench_main_idx_str_key ON argo_str_tbl (keystr) using bloomfilter;".replace("argo_str_tbl", Global.argo_str_tbl))
    #qs.query("CREATE INDEX argo_nobench_main_idx_str_val ON argo_str_tbl (valstr) using bloomfilter;".replace("argo_str_tbl", Global.argo_str_tbl))
    #qs.query("CREATE INDEX argo_nobench_main_idx_num_objid ON argo_num_tbl (objid) using csbtree;".replace("argo_num_tbl", Global.argo_num_tbl))
    #qs.query("CREATE INDEX argo_nobench_main_idx_num_key ON argo_num_tbl (keystr) using bloomfilter;".replace("argo_num_tbl", Global.argo_num_tbl))
    #qs.query("CREATE INDEX argo_nobench_main_idx_num_val ON argo_num_tbl (valnum) using csbtree;".replace("argo_num_tbl", Global.argo_num_tbl))
    #qs.query("CREATE INDEX argo_nobench_main_idx_num_val_sma ON argo_num_tbl (valnum) using sma;".replace("argo_num_tbl", Global.argo_num_tbl))
    #qs.query("CREATE INDEX argo_nobench_main_idx_bool_objid ON argo_bool_tbl (objid) using csbtree;".replace("argo_bool_tbl", Global.argo_bool_tbl))
    #qs.query("CREATE INDEX argo_nobench_main_idx_bool_key ON argo_bool_tbl (keystr) using bloomfilter;".replace("argo_bool_tbl", Global.argo_bool_tbl))


def load_data(input_filename, clean = True):
    if clean:
        clean_db()
    start = time.time()    
    str_file = input_filename + '_str.txt'
    num_file = input_filename + '_num.txt'
    bool_file = input_filename +  '_bool.txt'

    copy_str_sql = "copy {} FROM '{}' WITH (DELIMITER '|');".format(Global.argo_str_tbl, str_file)
    copy_num_sql = "copy {} FROM '{}'  WITH (DELIMITER '|');".format(Global.argo_num_tbl, num_file)
    copy_bool_sql = "copy {} FROM '{}' WITH (DELIMITER '|');".format(Global.argo_bool_tbl, bool_file)

    qs.query(copy_str_sql)
    qs.query(copy_num_sql)
    qs.query(copy_bool_sql)
    '''
    if clean:
        qs.query("CREATE INDEX argo_nobench_main_idx_str_objid ON argo_str_tbl (objid) using csbtree;")
        qs.query("CREATE INDEX argo_nobench_main_idx_str_key ON argo_str_tbl (keystr) using bloomfilter;")
        qs.query("CREATE INDEX argo_nobench_main_idx_str_val ON argo_str_tbl (valstr) using bloomfilter;")
        qs.query("CREATE INDEX argo_nobench_main_idx_num_objid ON argo_num_tbl (objid) using csbtree;")
        qs.query("CREATE INDEX argo_nobench_main_idx_num_key ON argo_num_tbl (keystr) using bloomfilter;")
        qs.query("CREATE INDEX argo_nobench_main_idx_num_val ON argo_num_tbl (valnum) using csbtree;")
        qs.query("CREATE INDEX argo_nobench_main_idx_bool_objid ON argo_bool_tbl (objid) using csbtree;")
        qs.query("CREATE INDEX argo_nobench_main_idx_bool_key ON argo_bool_tbl (keystr) using bloomfilter;")
    '''
    end = time.time()
    exec_time = round(end - start, 6)
    return exec_time

def process_sql(sql_query):
    res = qs.query(sql_query)
    someresults = False
    current_flatmap = {}
    query_time = res[-1]
    indx = 0
    if (len(res[1])) > 0:
        someresults = True
        valtuple = res[1][0]
        current_objid = valtuple[0]
    while indx < len(res[1]):
        valtuple = res[1][indx]
        if valtuple[0] != current_objid:
            current_objid = valtuple[0]
            yield reconstitute(current_flatmap)
            current_flatmap = {}
        if valtuple[2] != '-1':
            current_flatmap[valtuple[1]] = valtuple[2]
        elif valtuple[3] != '-1':
            current_flatmap[valtuple[1]] = int(valtuple[3])
        elif valtuple[4] != '-1':
            current_flatmap[valtuple[1]] = True if valtuple[4] == '1' else False
        indx += 1
    if someresults:
        yield reconstitute(current_flatmap)


def reconstitute(flatmap):
    reconmap = {}
    for k, v in flatmap.iteritems():
        reconstitute_kv(reconmap, k, v)
    return reconmap


def reconstitute_kv(reconmap, key, value):
    dot_pos = key.find(".")
    bracket_pos = key.find("[")
    if (dot_pos == -1) and (bracket_pos == -1):
        reconmap[key] = value
    elif dot_pos >= 0:
        if bracket_pos >= 0:
            if dot_pos < bracket_pos:
                prekey, postkey = key.split(".", 1)
                if not reconmap.has_key(prekey):
                    reconmap[prekey] = {}
                reconstitute_kv(reconmap[prekey], postkey, value)
            else:
                prekey, postfrag = key.split("[", 1)
                idxstr, postkey = postfrag.split("]", 1)
                if not reconmap.has_key(prekey):
                    reconmap[prekey] = []
                reconstitute_list(reconmap[prekey], int(idxstr), postkey, value)
        else:
            prekey, postkey = key.split(".", 1)
            if not reconmap.has_key(prekey):
                reconmap[prekey] = {}
            reconstitute_kv(reconmap[prekey], postkey, value)
    else:
        prekey, postfrag = key.split("[", 1)
        idxstr, postkey = postfrag.split("]", 1)
        if not reconmap.has_key(prekey):
            reconmap[prekey] = []
        reconstitute_list(reconmap[prekey], int(idxstr), postkey, value)


def reconstitute_list(reconlist, idx, key, value):
    while not (idx < len(reconlist)):
        reconlist.append(None)
    if len(key) == 0:
        reconlist[idx] = value
    elif key[0] == '.':
        if reconlist[idx] == None:
            reconlist[idx] = {}
        reconstitute_kv(reconlist[idx], key[1:], value)
    else:
        idxstr, postkey = key[1:].split("]", 1)
        if reconlist[idx] == None:
            reconlist[idx] = []
        reconstitute_list(reconlist[idx], int(idxstr), postkey, value)


def process_agg_sql(sql_query):
    res = qs.query(sql_query)
    column_names = res[0]
    valtuple = None
    indx = 0
    while indx < len(res[1]):
        valtuple = res[1][indx]
        row_json = {}
        for col_id in range(len(column_names)):
            row_json[column_names[col_id]] = int(valtuple[col_id])
            #res_json.setdefault('result',[]).append(row_json)
        indx += 1
        yield(row_json)

def replace_table_names(queryset):
    for indx in range(len(queryset)):
        query = queryset[indx]
        queryset[indx] = query.replace('argo_str_tbl', Global.argo_str_tbl).replace('argo_num_tbl', Global.argo_num_tbl).replace('argo_bool_tbl', Global.argo_bool_tbl)
    return queryset

def main(argv):
    #query_params = [[], [], ['11'], ["11","22"], ["GBRDCMJRGEYTC==="], ["100", "200"], ["100", "200"], ["the"], ["500", "GBRDCMJQ"], ["100", "200"]] 
    query_params = argv 
    query1 = ["DROP TABLE argo_im;",
            "CREATE TABLE argo_im (objid bigint, keystr varchar(100), valstr varchar(100), valnum double, valbool INTEGER);",
            "Insert into argo_im SELECT objid, keystr, valstr, -1, -1 FROM argo_str_tbl WHERE (keystr = 'str1' OR keystr LIKE 'str1.%' OR keystr LIKE 'str1[%' OR keystr = 'num' OR keystr LIKE 'num.%' OR keystr LIKE 'num[%');",
            "Insert into argo_im SELECT objid, keystr, '-1', valnum, -1 FROM argo_num_tbl WHERE (keystr = 'str1' OR keystr LIKE 'str1.%' OR keystr LIKE 'str1[%' OR keystr = 'num' OR keystr LIKE 'num.%' OR keystr LIKE 'num[%');",
            "Insert into argo_im SELECT objid, keystr, '-1', -1, valbool FROM argo_bool_tbl WHERE (keystr = 'str1' OR keystr LIKE 'str1.%' OR keystr LIKE 'str1[%' OR keystr = 'num' OR keystr LIKE 'num.%' OR keystr LIKE 'num[%');",
            "Select * from argo_im order by objid;"]

    query2 = ["DROP TABLE argo_im;",
            "CREATE TABLE argo_im (objid bigint, keystr varchar(100), valstr varchar(100), valnum double, valbool INTEGER);",
            "Insert into argo_im SELECT objid, keystr, valstr, -1, -1 FROM argo_str_tbl WHERE (keystr = 'nested_obj.str' OR keystr LIKE 'nested_obj.str.%' OR keystr LIKE 'nested_obj.str[%' OR keystr = 'nested_obj.num' OR keystr LIKE 'nested_obj.num.%' OR keystr LIKE 'nested_obj.num[%');",
            "Insert into argo_im SELECT objid, keystr, '-1', valnum, -1 FROM argo_num_tbl WHERE (keystr = 'nested_obj.str' OR keystr LIKE 'nested_obj.str.%' OR keystr LIKE 'nested_obj.str[%' OR keystr = 'nested_obj.num' OR keystr LIKE 'nested_obj.num.%' OR keystr LIKE 'nested_obj.num[%');",
            "Insert into argo_im SELECT objid, keystr, '-1', -1, valbool FROM argo_bool_tbl WHERE (keystr = 'nested_obj.str' OR keystr LIKE 'nested_obj.str.%' OR keystr LIKE 'nested_obj.str[%' OR keystr = 'nested_obj.num' OR keystr LIKE 'nested_obj.num.%' OR keystr LIKE 'nested_obj.num[%');",
            "Select * from argo_im order by objid;"]
    params = query_params[2]
    query3 = ["DROP TABLE argo_im;",
            "CREATE TABLE argo_im (objid bigint, keystr varchar(100), valstr varchar(100), valnum double, valbool INTEGER);",
            string.replace("Insert into argo_im SELECT objid, keystr, valstr, -1, -1 FROM argo_str_tbl WHERE (keystr = 'sparse_xxxxx0' OR keystr LIKE 'sparse_xxxxx0.%' OR keystr LIKE 'sparse_xxxxx0[%' OR keystr = 'sparse_xxxxx9' OR keystr LIKE 'sparse_xxxxx0.%' OR keystr LIKE 'sparse_xxxxx9[%');", "xxxxx", params[0]),
            string.replace("Insert into argo_im SELECT objid, keystr, '-1', valnum, -1 FROM argo_num_tbl WHERE (keystr = 'sparse_xxxxx0' OR keystr LIKE 'sparse_xxxxx0.%' OR keystr LIKE 'sparse_xxxxx0[%' OR keystr = 'sparse_xxxxx9' OR keystr LIKE 'sparse_xxxxx9.%' OR keystr LIKE 'sparse_xxxxx9[%');", "xxxxx", params[0]),
            string.replace("Insert into argo_im SELECT objid, keystr, '-1', -1, valbool FROM argo_bool_tbl WHERE (keystr = 'sparse_xxxxx0' OR keystr LIKE 'sparse_xxxxx0.%' OR keystr LIKE 'sparse_xxxxx0[%' OR keystr = 'sparse_xxxxx9' OR keystr LIKE 'sparse_xxxxx9.%' OR keystr LIKE 'sparse_xxxxx9[%');", "xxxxx", params[0]),
            "Select * from argo_im order by objid;"]
    #print query3

    params = query_params[3]
    query4 = ["DROP TABLE argo_im;",
            "CREATE TABLE argo_im (objid bigint, keystr varchar(100), valstr varchar(100), valnum double, valbool INTEGER);",
            string.replace("Insert into argo_im SELECT objid, keystr, valstr, -1, -1 FROM argo_str_tbl WHERE (keystr = 'sparse_xxxxx0' OR keystr LIKE 'sparse_xxxxx0.%' OR keystr LIKE 'sparse_xxxxx0[%' OR keystr = 'sparse_yyyyy0' OR keystr LIKE 'sparse_yyyyy0.%' OR keystr LIKE 'sparse_yyyyy0[%');", "xxxxx", params[0]).replace("yyyyy", params[1]),
            string.replace("Insert into argo_im SELECT objid, keystr, '-1', valnum, -1 FROM argo_num_tbl WHERE (keystr = 'sparse_xxxxx0' OR keystr LIKE 'sparse_xxxxx0.%' OR keystr LIKE 'sparse_xxxxx0[%' OR keystr = 'sparse_yyyyy0' OR keystr LIKE 'sparse_yyyyy0.%' OR keystr LIKE 'sparse_yyyyy0[%');", "xxxxx", params[0]).replace("yyyyy", params[1]),
            string.replace("Insert into argo_im SELECT objid, keystr, '-1', -1, valbool FROM argo_bool_tbl WHERE (keystr = 'sparse_xxxxx0' OR keystr LIKE 'sparse_xxxxx0.%' OR keystr LIKE 'sparse_xxxxx0[%' OR keystr = 'sparse_yyyyy0' OR keystr LIKE 'sparse_yyyyy0.%' OR keystr LIKE 'sparse_yyyyy0[%');", "xxxxx", params[0]).replace("yyyyy", params[1]),
            "Select * from argo_im order by objid;"]
    #print query4
    params = query_params[4]
    query5 = ["DROP TABLE argo_select;", "CREATE TABLE argo_select (objid bigint);",
            "INSERT into argo_select SELECT objid FROM argo_str_tbl WHERE (keystr = 'str1') AND (valstr = '{}') group by objid;".format(params[0]),
            "DROP TABLE argo_im;",
            "CREATE TABLE argo_im (objid bigint, keystr varchar(100), valstr varchar(100), valnum double, valbool INTEGER);",
            "Insert into argo_im SELECT objid, keystr, valstr, -1, -1 FROM argo_str_tbl WHERE objid in (Select * from argo_select);",
            "Insert into argo_im SELECT objid, keystr, '-1', valnum, -1 FROM argo_num_tbl WHERE objid in (Select * from argo_select);",
            "Insert into argo_im SELECT objid, keystr, '-1', -1, valbool FROM argo_bool_tbl WHERE objid in (Select * from argo_select);",
            "Select * from argo_im order by objid;"]
    #print query5
    params = query_params[5]
    query6 = ["DROP TABLE argo_select;", "CREATE TABLE argo_select (objid bigint);",
            "INSERT into argo_select SELECT objid FROM argo_num_tbl WHERE (keystr = 'num') AND (valnum >= {} and valnum <= {}) group by objid;".format(params[0], params[1]),
            "DROP TABLE argo_im;",
            "CREATE TABLE argo_im (objid bigint, keystr varchar(100), valstr varchar(100), valnum double, valbool INTEGER);",
            "Insert into argo_im SELECT objid, keystr, valstr, -1, -1 FROM argo_str_tbl WHERE objid in (Select * from argo_select);",
            "Insert into argo_im SELECT objid, keystr, '-1', valnum, -1 FROM argo_num_tbl WHERE objid in (Select * from argo_select);",
            "Insert into argo_im SELECT objid, keystr, '-1', -1, valbool FROM argo_bool_tbl WHERE objid in (Select * from argo_select);",
            "Select * from argo_im order by objid;"]
    #print query6
    params = query_params[6]
    query7 = ["DROP TABLE argo_select;",
            "CREATE TABLE argo_select (objid bigint);",
            "INSERT into argo_select SELECT objid FROM argo_num_tbl WHERE (keystr = 'dyn1') AND (valnum >= {} and valnum <= {}) group by objid;".format(params[0], params[1]),
            "DROP TABLE argo_im;",
            "CREATE TABLE argo_im (objid bigint, keystr varchar(100), valstr varchar(100), valnum double, valbool INTEGER);",
            "Insert into argo_im SELECT objid, keystr, valstr, -1, -1 FROM argo_str_tbl WHERE objid in (Select * from argo_select);",
            "Insert into argo_im SELECT objid, keystr, '-1', valnum, -1 FROM argo_num_tbl WHERE objid in (Select * from argo_select);",
            "Insert into argo_im SELECT objid, keystr, '-1', -1, valbool FROM argo_bool_tbl WHERE objid in (Select * from argo_select);",
            "Select * from argo_im order by objid;"]
    #print query7
    params = query_params[7]
    query8 = ["DROP TABLE argo_select;",
            "CREATE TABLE argo_select (objid bigint);"
            "INSERT into argo_select SELECT objid FROM argo_str_tbl WHERE (keystr like 'nested_arr[%]') AND (valstr = '{}')  group by objid;".format(params[0]),
            "DROP TABLE argo_im;",
            "CREATE TABLE argo_im (objid bigint, keystr varchar(100), valstr varchar(100), valnum double, valbool INTEGER);",
            "Insert into argo_im SELECT objid, keystr, valstr, -1, -1 FROM argo_str_tbl WHERE objid in (Select * from argo_select);",
            "Insert into argo_im SELECT objid, keystr, '-1', valnum, -1 FROM argo_num_tbl WHERE objid in (Select * from argo_select);",
            "Insert into argo_im SELECT objid, keystr, '-1', -1, valbool FROM argo_bool_tbl WHERE objid in (Select * from argo_select);",
            "Select * from argo_im order by objid;"]
    #print query8
    params = query_params[8]
    query9 = ["DROP TABLE argo_select;",
            "CREATE TABLE argo_select (objid bigint);",
            "INSERT into argo_select SELECT objid FROM argo_str_tbl WHERE (keystr = 'sparse_{}') AND (valstr = '{}') group by objid;".format(params[0], params[1]),
            "DROP TABLE argo_im;",
            "CREATE TABLE argo_im (objid bigint, keystr varchar(100), valstr varchar(100), valnum double, valbool INTEGER);",
            "Insert into argo_im SELECT objid, keystr, valstr, -1, -1 FROM argo_str_tbl WHERE objid in (Select * from argo_select);",
            "Insert into argo_im SELECT objid, keystr, '-1', valnum, -1 FROM argo_num_tbl WHERE objid in (Select * from argo_select);",
            "Insert into argo_im SELECT objid, keystr, '-1', -1, valbool FROM argo_bool_tbl WHERE objid in (Select * from argo_select);",
            "Select * from argo_im order by objid;"]
    #print query9

    params = query_params[9]
    query10 = ["DROP TABLE argo_select;",
            "CREATE TABLE argo_select (objid bigint);",
            "INSERT into argo_select SELECT objid FROM argo_num_tbl WHERE (keystr = 'num') AND (valnum >= {} and valnum <= {});".format(params[0], params[1]),
            "SELECT valnum as thousandth, count(*) as count FROM argo_num_tbl WHERE objid in (SELECT objid FROM argo_select) AND keystr = 'thousandth' GROUP BY valnum;"]

    params = query_params[10]
    query11 = ["DROP TABLE argo_select;",
            "CREATE TABLE argo_select (objid bigint);",
            "INSERT into argo_select SELECT objid FROM argo_num_tbl WHERE keystr = 'num' and (valnum >= {} and valnum <= {});".format(params[0], params[1]),
            "SELECT * FROM argo_str_tbl AS argo_join_left, argo_str_tbl AS argo_join_right WHERE argo_join_left.keystr = 'nested_obj.str' AND argo_join_right.keystr = 'str1' AND argo_join_left.valstr = argo_join_right.valstr AND argo_join_left.objid in (SELECT objid FROM argo_select);"]

    queries = [query1, query2, query3, query4, query5, query6, query7, query8, query9, query10, query11]

    for indx in range(len(queries)):
        query = queries[indx]
        queries[indx]= replace_table_names(query)

    results = ['quickstep']

    for idx, val in enumerate(queries):
        #output_file = 'quickstep_query' + str(idx+1) + '.json'
        #fp = open(output_file, 'w')
        start = time.time()
        count = 0
        for query in val:
            try:
                #print 'executing:', query
                if idx < 9 or idx == 10:
                    res = process_sql(query)
                elif idx == 9:
                    res = process_agg_sql(query)
                for row in res:
                    #fp.write(json.dumps(row, sort_keys=True))
                    #fp.write('\n')
                    count += 1
            except:
                print 'bad query:', query
        end = time.time()
        query_time = "%.6f" % (end - start)
        print 'query', str(idx+1), 'time:', query_time, 'count:', count
        results.append(query_time)
        #fp.close()
    print results
    return results        

if __name__ == "__main__":
    main(sys.argv[1:])
