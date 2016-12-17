#!/usr/bin/python
import json, sys
import psycopg2
import time
import subprocess
import string
import Global

POSTGRES_USER='rashij'
postgres_db = psycopg2.connect("dbname=argo user=rashij")

def clean_db():
    cursor = postgres_db.cursor()
    cursor.execute("DROP TABLE IF EXISTS {};".format(Global.argo_str_tbl))
    cursor.execute("DROP TABLE IF EXISTS {};".format(Global.argo_num_tbl))
    cursor.execute("DROP TABLE IF EXISTS {};".format(Global.argo_bool_tbl))
    cursor.execute("create table {} (objid bigint, keystr text, valstr text)".format(Global.argo_str_tbl))
    cursor.execute("create table {} (objid bigint, keystr text, valnum float)".format(Global.argo_num_tbl))
    cursor.execute("create table {} (objid bigint, keystr text, valbool bool)".format(Global.argo_bool_tbl))
    '''
    cursor.execute("CREATE INDEX argo_nobench_main_idx_str_objid ON {} (objid)".format(Global.argo_str_tbl))
    cursor.execute("CREATE INDEX argo_nobench_main_idx_str_key ON {} (keystr)".format(Global.argo_str_tbl))
    cursor.execute("CREATE INDEX argo_nobench_main_idx_str_val ON {} (valstr)".format(Global.argo_str_tbl))
    cursor.execute("CREATE INDEX argo_nobench_main_idx_num_objid ON {} (objid)".format(Global.argo_num_tbl))
    cursor.execute("CREATE INDEX argo_nobench_main_idx_num_key ON {} (keystr)".format(Global.argo_num_tbl))
    cursor.execute("CREATE INDEX argo_nobench_main_idx_num_val ON {} (valnum)".format(Global.argo_num_tbl))
    cursor.execute("CREATE INDEX argo_nobench_main_idx_bool_objid ON {} (objid)".format(Global.argo_bool_tbl))
    cursor.execute("CREATE INDEX argo_nobench_main_idx_bool_key ON {} (keystr)".format(Global.argo_bool_tbl))
    '''
    postgres_db.commit()
    cursor.close()


def load_data(input_filename, clean=True):
    if clean:
        clean_db()
    start = time.time()

    str_file = input_filename + '_str.txt'
    num_file = input_filename + '_num.txt'
    bool_file = input_filename +  '_bool.txt'

    load_str = subprocess.Popen(["psql", "-w", "-U", POSTGRES_USER, "-d", "argo", "-c",
                                 "\COPY {}(objid, keystr, valstr) FROM ".format(Global.argo_str_tbl) + str_file + " WITH DELIMITER '|';"],
                                 stdout=subprocess.PIPE)

    load_num = subprocess.Popen(["psql", "-w", "-U", POSTGRES_USER, "-d", "argo", "-c",
                                 "\COPY {}(objid, keystr, valnum) FROM ".format(Global.argo_num_tbl) + num_file + " WITH DELIMITER '|';"],
                                 stdout=subprocess.PIPE)

    load_bool = subprocess.Popen(["psql", "-w", "-U", POSTGRES_USER, "-d", "argo", "-c",
                                  "\COPY {}(objid, keystr, valbool) FROM ".format(Global.argo_bool_tbl) + bool_file + " WITH DELIMITER '|';"], 
                                  stdout=subprocess.PIPE)

    load_bool.communicate()
    load_num.communicate()
    load_str.communicate()

    end = time.time()
    exec_time = round(end - start, 6)
    return exec_time


def process_sql(sql_query, select=True):
    cursor = postgres_db.cursor()
    cursor.execute(sql_query)

    someresults = cursor.rowcount > 0
    valtuple = None
    if someresults and select:
        valtuple = cursor.fetchone()
        someresults = True
        current_flatmap = {} 
        current_objid = valtuple[0]
    while valtuple:
        if valtuple[0] != current_objid:
            current_objid = valtuple[0]
            yield reconstitute(current_flatmap)
            current_flatmap = {}
        if valtuple[2] != None:
            current_flatmap[valtuple[1]] = valtuple[2]
        elif valtuple[3] != None:
            current_flatmap[valtuple[1]] = int(valtuple[3])
        elif valtuple[4] != None:
            current_flatmap[valtuple[1]] = valtuple[4]
        valtuple = cursor.fetchone()
    if someresults and select:
        yield reconstitute(current_flatmap)
    cursor.close()


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


def process_agg_sql(sql_query, select=True):
    cursor = postgres_db.cursor()
    cursor.execute(sql_query)

    if select:
        column_names = [desc[0] for desc in cursor.description]
        someresults = cursor.rowcount > 0
        valtuple = None
        if someresults:
            valtuple = cursor.fetchone()
        while valtuple:
            row_json = {}
            for col_id in range(len(column_names)):
                row_json[column_names[col_id]] = int(valtuple[col_id])
            #res_json.setdefault('result',[]).append(row_json)
            valtuple = cursor.fetchone()
            yield(row_json)
    cursor.close()

def replace_table_names(queryset):
    for indx in range(len(queryset)):
        query = queryset[indx]
        queryset[indx] = query.replace('argo_str_tbl', Global.argo_str_tbl).replace('argo_num_tbl', Global.argo_num_tbl).replace('argo_bool_tbl', Global.argo_bool_tbl)
    return queryset 

def main(argv):
    query_params = argv

    '''
    query1 = ["DROP TABLE IF EXISTS argo_im;",
            "CREATE TABLE argo_im (objid bigint, keystr text, valstr text, valnum float, valbool bool);",
            "Insert into argo_im SELECT objid, keystr, valstr, CAST (NULL AS DOUBLE PRECISION), CAST (NULL AS BOOLEAN) FROM argo_str_tbl WHERE (keystr = 'str1' OR keystr LIKE 'str1.%' OR keystr LIKE 'str1[%' OR keystr = 'num' OR keystr LIKE 'num.%' OR keystr LIKE 'num[%');",
            "Insert into argo_im SELECT objid, keystr, 'CAST (NULL AS TEXT)', valnum, CAST (NULL AS BOOLEAN) FROM argo_num_tbl WHERE (keystr = 'str1' OR keystr LIKE 'str1.%' OR keystr LIKE 'str1[%' OR keystr = 'num' OR keystr LIKE 'num.%' OR keystr LIKE 'num[%');",
            "Insert into argo_im SELECT objid, keystr, 'CAST (NULL AS TEXT)', CAST (NULL AS DOUBLE PRECISION), valbool FROM argo_bool_tbl WHERE (keystr = 'str1' OR keystr LIKE 'str1.%' OR keystr LIKE 'str1[%' OR keystr = 'num' OR keystr LIKE 'num.%' OR keystr LIKE 'num[%');",
            "SELECT * from argo_im order by objid;"]

    query2 = ["DROP TABLE argo_im;",
            "CREATE TABLE argo_im (objid bigint, keystr text, valstr text, valnum float, valbool bool);",
            "Insert into argo_im SELECT objid, keystr, valstr, CAST (NULL AS DOUBLE PRECISION), CAST (NULL AS BOOLEAN) FROM argo_str_tbl WHERE (keystr = 'nested_obj.str' OR keystr LIKE 'nested_obj.str.%' OR keystr LIKE 'nested_obj.str[%' OR keystr = 'nested_obj.num' OR keystr LIKE 'nested_obj.num.%' OR keystr LIKE 'nested_obj.num[%');",
            "Insert into argo_im SELECT objid, keystr, 'CAST (NULL AS TEXT)', valnum, CAST (NULL AS BOOLEAN) FROM argo_num_tbl WHERE (keystr = 'nested_obj.str' OR keystr LIKE 'nested_obj.str.%' OR keystr LIKE 'nested_obj.str[%' OR keystr = 'nested_obj.num' OR keystr LIKE 'nested_obj.num.%' OR keystr LIKE 'nested_obj.num[%');",
            "Insert into argo_im SELECT objid, keystr, 'CAST (NULL AS TEXT)', CAST (NULL AS DOUBLE PRECISION), valbool FROM argo_bool_tbl WHERE (keystr = 'nested_obj.str' OR keystr LIKE 'nested_obj.str.%' OR keystr LIKE 'nested_obj.str[%' OR keystr = 'nested_obj.num' OR keystr LIKE 'nested_obj.num.%' OR keystr LIKE 'nested_obj.num[%');",
            "SELECT * from argo_im order by objid;"]
    params = query_params[2]
    query3 = ["DROP TABLE argo_im;",
            "CREATE TABLE argo_im (objid bigint, keystr text, valstr text, valnum float, valbool bool);",
            string.replace("Insert into argo_im SELECT objid, keystr, valstr, CAST (NULL AS DOUBLE PRECISION), CAST (NULL AS BOOLEAN) FROM argo_str_tbl WHERE (keystr = 'sparse_xxxxx0' OR keystr LIKE 'sparse_xxxxx0.%' OR keystr LIKE 'sparse_xxxxx0[%' OR keystr = 'sparse_xxxxx9' OR keystr LIKE 'sparse_xxxxx0.%' OR keystr LIKE 'sparse_xxxxx9[%');", "xxxxx", params[0]),
            string.replace("Insert into argo_im SELECT objid, keystr, 'CAST (NULL AS TEXT)', valnum, CAST (NULL AS BOOLEAN) FROM argo_num_tbl WHERE (keystr = 'sparse_xxxxx0' OR keystr LIKE 'sparse_xxxxx0.%' OR keystr LIKE 'sparse_xxxxx0[%' OR keystr = 'sparse_xxxxx9' OR keystr LIKE 'sparse_xxxxx9.%' OR keystr LIKE 'sparse_xxxxx9[%');", "xxxxx", params[0]),
            string.replace("Insert into argo_im SELECT objid, keystr, 'CAST (NULL AS TEXT)', CAST (NULL AS DOUBLE PRECISION), valbool FROM argo_bool_tbl WHERE (keystr = 'sparse_xxxxx0' OR keystr LIKE 'sparse_xxxxx0.%' OR keystr LIKE 'sparse_xxxxx0[%' OR keystr = 'sparse_xxxxx9' OR keystr LIKE 'sparse_xxxxx9.%' OR keystr LIKE 'sparse_xxxxx9[%');", "xxxxx", params[0]),
            "SELECT * from argo_im order by objid;"]
    #print query3

    params = query_params[3]
    query4 = ["DROP TABLE argo_im;",
            "CREATE TABLE argo_im (objid bigint, keystr text, valstr text, valnum float, valbool bool);",
            string.replace("Insert into argo_im SELECT objid, keystr, valstr, CAST (NULL AS DOUBLE PRECISION), CAST (NULL AS BOOLEAN) FROM argo_str_tbl WHERE (keystr = 'sparse_xxxxx0' OR keystr LIKE 'sparse_xxxxx0.%' OR keystr LIKE 'sparse_xxxxx0[%' OR keystr = 'sparse_yyyyy0' OR keystr LIKE 'sparse_yyyyy0.%' OR keystr LIKE 'sparse_yyyyy0[%');", "xxxxx", params[0]).replace("yyyyy", params[1]),
            string.replace("Insert into argo_im SELECT objid, keystr, 'CAST (NULL AS TEXT)', valnum, CAST (NULL AS BOOLEAN) FROM argo_num_tbl WHERE (keystr = 'sparse_xxxxx0' OR keystr LIKE 'sparse_xxxxx0.%' OR keystr LIKE 'sparse_xxxxx0[%' OR keystr = 'sparse_yyyyy0' OR keystr LIKE 'sparse_yyyyy0.%' OR keystr LIKE 'sparse_yyyyy0[%');", "xxxxx", params[0]).replace("yyyyy", params[1]),
            string.replace("Insert into argo_im SELECT objid, keystr, 'CAST (NULL AS TEXT)', CAST (NULL AS DOUBLE PRECISION), valbool FROM argo_bool_tbl WHERE (keystr = 'sparse_xxxxx0' OR keystr LIKE 'sparse_xxxxx0.%' OR keystr LIKE 'sparse_xxxxx0[%' OR keystr = 'sparse_yyyyy0' OR keystr LIKE 'sparse_yyyyy0.%' OR keystr LIKE 'sparse_yyyyy0[%');", "xxxxx", params[0]).replace("yyyyy", params[1]),
            "SELECT * from argo_im order by objid;"]
    #print query4
    params = query_params[4]
    query5 = ["DROP TABLE IF EXISTS argo_select;", "CREATE TABLE argo_select (objid bigint);",
            "INSERT into argo_select SELECT objid FROM argo_str_tbl WHERE (keystr = 'str1') AND (valstr = '{}') group by objid;".format(params[0]),
            "DROP TABLE argo_im;",
            "CREATE TABLE argo_im (objid bigint, keystr text, valstr text, valnum float, valbool bool);",
            "Insert into argo_im SELECT objid, keystr, valstr, CAST (NULL AS DOUBLE PRECISION), CAST (NULL AS BOOLEAN) FROM argo_str_tbl WHERE objid in (SELECT * from argo_select);",
            "Insert into argo_im SELECT objid, keystr, 'CAST (NULL AS TEXT)', valnum, CAST (NULL AS BOOLEAN) FROM argo_num_tbl WHERE objid in (SELECT * from argo_select);",
            "Insert into argo_im SELECT objid, keystr, 'CAST (NULL AS TEXT)', CAST (NULL AS DOUBLE PRECISION), valbool FROM argo_bool_tbl WHERE objid in (SELECT * from argo_select);",
            "SELECT * from argo_im order by objid;"]
    #print query5
    params = query_params[5]
    query6 = ["DROP TABLE argo_select;", "CREATE TABLE argo_select (objid bigint);",
            "INSERT into argo_select SELECT objid FROM argo_num_tbl WHERE (keystr = 'num') AND (valnum >= {} and valnum <= {}) group by objid;".format(params[0], params[1]),
            "DROP TABLE argo_im;",
            "CREATE TABLE argo_im (objid bigint, keystr text, valstr text, valnum float, valbool bool);",
            "Insert into argo_im SELECT objid, keystr, valstr, CAST (NULL AS DOUBLE PRECISION), CAST (NULL AS BOOLEAN) FROM argo_str_tbl WHERE objid in (SELECT * from argo_select);",
            "Insert into argo_im SELECT objid, keystr, 'CAST (NULL AS TEXT)', valnum, CAST (NULL AS BOOLEAN) FROM argo_num_tbl WHERE objid in (SELECT * from argo_select);",
            "Insert into argo_im SELECT objid, keystr, 'CAST (NULL AS TEXT)', CAST (NULL AS DOUBLE PRECISION), valbool FROM argo_bool_tbl WHERE objid in (SELECT * from argo_select);",
            "SELECT * from argo_im order by objid;"]
    #print query6
    params = query_params[6]
    query7 = ["DROP TABLE argo_select;",
            "CREATE TABLE argo_select (objid bigint);",
            "INSERT into argo_select SELECT objid FROM argo_num_tbl WHERE (keystr = 'dyn1') AND (valnum >= {} and valnum <= {}) group by objid;".format(params[0], params[1]),
            "DROP TABLE argo_im;",
            "CREATE TABLE argo_im (objid bigint, keystr text, valstr text, valnum float, valbool bool);",
            "Insert into argo_im SELECT objid, keystr, valstr, CAST (NULL AS DOUBLE PRECISION), CAST (NULL AS BOOLEAN) FROM argo_str_tbl WHERE objid in (SELECT * from argo_select);",
            "Insert into argo_im SELECT objid, keystr, 'CAST (NULL AS TEXT)', valnum, CAST (NULL AS BOOLEAN) FROM argo_num_tbl WHERE objid in (SELECT * from argo_select);",
            "Insert into argo_im SELECT objid, keystr, 'CAST (NULL AS TEXT)', CAST (NULL AS DOUBLE PRECISION), valbool FROM argo_bool_tbl WHERE objid in (SELECT * from argo_select);",
            "SELECT * from argo_im order by objid;"]
    #print query7
    params = query_params[7]
    query8 = ["DROP TABLE argo_select;",
            "CREATE TABLE argo_select (objid bigint);"
            "INSERT into argo_select SELECT objid FROM argo_str_tbl WHERE (keystr like 'nested_arr[%]') AND (valstr = '{}')  group by objid;".format(params[0]),
            "DROP TABLE argo_im;",
            "CREATE TABLE argo_im (objid bigint, keystr text, valstr text, valnum float, valbool bool);",
            "Insert into argo_im SELECT objid, keystr, valstr, CAST (NULL AS DOUBLE PRECISION), CAST (NULL AS BOOLEAN) FROM argo_str_tbl WHERE objid in (SELECT * from argo_select);",
            "Insert into argo_im SELECT objid, keystr, 'CAST (NULL AS TEXT)', valnum, CAST (NULL AS BOOLEAN) FROM argo_num_tbl WHERE objid in (SELECT * from argo_select);",
            "Insert into argo_im SELECT objid, keystr, 'CAST (NULL AS TEXT)', CAST (NULL AS DOUBLE PRECISION), valbool FROM argo_bool_tbl WHERE objid in (SELECT * from argo_select);",
            "SELECT * from argo_im order by objid;"]
    #print query8
    params = query_params[8]
    query9 = ["DROP TABLE argo_select;",
            "CREATE TABLE argo_select (objid bigint);",
            "INSERT into argo_select SELECT objid FROM argo_str_tbl WHERE (keystr = 'sparse_{}') AND (valstr = '{}') group by objid;".format(params[0], params[1]),
            "DROP TABLE argo_im;",
            "CREATE TABLE argo_im (objid bigint, keystr text, valstr text, valnum float, valbool bool);",
            "Insert into argo_im SELECT objid, keystr, valstr, CAST (NULL AS DOUBLE PRECISION), CAST (NULL AS BOOLEAN) FROM argo_str_tbl WHERE objid in (SELECT * from argo_select);",
            "Insert into argo_im SELECT objid, keystr, 'CAST (NULL AS TEXT)', valnum, CAST (NULL AS BOOLEAN) FROM argo_num_tbl WHERE objid in (SELECT * from argo_select);",
            "Insert into argo_im SELECT objid, keystr, 'CAST (NULL AS TEXT)', CAST (NULL AS DOUBLE PRECISION), valbool FROM argo_bool_tbl WHERE objid in (SELECT * from argo_select);",
            "SELECT * from argo_im order by objid;"]
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
    '''
    query1 = [
        "SELECT objid, keystr, valstr, CAST (NULL AS DOUBLE PRECISION), CAST (NULL AS BOOLEAN) FROM argo_str_tbl WHERE keystr = 'str1' OR keystr LIKE 'str1.%' OR keystr LIKE 'str1[%' OR keystr = 'num' OR keystr LIKE 'num.%' OR keystr LIKE 'num[%' UNION SELECT objid, keystr, CAST (NULL AS TEXT), valnum, CAST (NULL AS BOOLEAN) FROM argo_num_tbl WHERE keystr = 'str1' OR keystr LIKE 'str1.%' OR keystr LIKE 'str1[%' OR keystr = 'num' OR keystr LIKE 'num.%' OR keystr LIKE 'num[%' UNION SELECT objid, keystr, CAST (NULL AS TEXT), CAST (NULL AS DOUBLE PRECISION), valbool FROM argo_bool_tbl WHERE keystr = 'str1' OR keystr LIKE 'str1.%' OR keystr LIKE 'str1[%' OR keystr = 'num' OR keystr LIKE 'num.%' OR keystr LIKE 'num[%' order by objid;"]

    query2 = [
        "SELECT objid, keystr, valstr, CAST (NULL AS DOUBLE PRECISION), CAST (NULL AS BOOLEAN) FROM argo_str_tbl WHERE keystr = 'nested_obj.str' OR keystr LIKE 'nested_obj.str.%' OR keystr LIKE 'nested_obj.str[%' OR keystr = 'nested_obj.num' OR keystr LIKE 'nested_obj.num.%' OR keystr LIKE 'nested_obj.num[%' UNION SELECT objid, keystr, CAST (NULL AS TEXT), valnum, CAST (NULL AS BOOLEAN) FROM argo_num_tbl WHERE keystr = 'nested_obj.str' OR keystr LIKE 'nested_obj.str.%' OR keystr LIKE 'nested_obj.str[%' OR keystr = 'nested_obj.num' OR keystr LIKE 'nested_obj.num.%' OR keystr LIKE 'nested_obj.num[%' UNION SELECT objid, keystr, CAST (NULL AS TEXT), CAST (NULL AS DOUBLE PRECISION), valbool FROM argo_bool_tbl WHERE keystr = 'nested_obj.str' OR keystr LIKE 'nested_obj.str.%' OR keystr LIKE 'nested_obj.str[%' OR keystr = 'nested_obj.num' OR keystr LIKE 'nested_obj.num.%' OR keystr LIKE 'nested_obj.num[%'  order by objid;"]

    params = query_params[2]
    query3 = [
        string.replace("SELECT objid, keystr, valstr, CAST (NULL AS DOUBLE PRECISION), CAST (NULL AS BOOLEAN) FROM argo_str_tbl WHERE keystr = 'sparse_xxxxx0' OR keystr LIKE 'sparse_xxxxx0.%' OR keystr LIKE 'sparse_xxxxx0[%' OR keystr = 'sparse_xxxxx9' OR keystr LIKE 'sparse_xxxxx9.%' OR keystr LIKE 'sparse_xxxxx9[%' UNION SELECT objid, keystr, CAST (NULL AS TEXT), valnum, CAST (NULL AS BOOLEAN) FROM argo_num_tbl WHERE keystr = 'sparse_xxxxx0' OR keystr LIKE 'sparse_xxxxx0.%' OR keystr LIKE 'sparse_xxxxx0[%' OR keystr = 'sparse_xxxxx9' OR keystr LIKE 'sparse_xxxxx9.%' OR keystr LIKE 'sparse_xxxxx9[%' UNION SELECT objid, keystr, CAST (NULL AS TEXT), CAST (NULL AS DOUBLE PRECISION), valbool FROM argo_bool_tbl WHERE keystr = 'sparse_xxxxx0' OR keystr LIKE 'sparse_xxxxx0.%' OR keystr LIKE 'sparse_xxxxx0[%' OR keystr = 'sparse_xxxxx9' OR keystr LIKE 'sparse_xxxxx9.%' OR keystr LIKE 'sparse_xxxxx9[%' order by objid;", "xxxxx", params[0])]

    params = query_params[3]
    query4 = [
        string.replace("SELECT objid, keystr, valstr, CAST (NULL AS DOUBLE PRECISION), CAST (NULL AS BOOLEAN) FROM argo_str_tbl WHERE keystr = 'sparse_xxxxx0' OR keystr LIKE 'sparse_xxxxx0.%' OR keystr LIKE 'sparse_xxxxx0[%' OR keystr = 'sparse_yyyyy0' OR keystr LIKE 'sparse_yyyyy0.%' OR keystr LIKE 'sparse_yyyyy0[%' UNION SELECT objid, keystr, CAST (NULL AS TEXT), valnum, CAST (NULL AS BOOLEAN) FROM argo_num_tbl WHERE keystr = 'sparse_xxxxx0' OR keystr LIKE 'sparse_xxxxx0.%' OR keystr LIKE 'sparse_xxxxx0[%' OR keystr = 'sparse_yyyyy0' OR keystr LIKE 'sparse_yyyyy0.%' OR keystr LIKE 'sparse_yyyyy0[%' UNION SELECT objid, keystr, CAST (NULL AS TEXT), CAST (NULL AS DOUBLE PRECISION), valbool FROM argo_bool_tbl WHERE keystr = 'sparse_xxxxx0' OR keystr LIKE 'sparse_xxxxx0.%' OR keystr LIKE 'sparse_xxxxx0[%' OR keystr = 'sparse_yyyyy0' OR keystr LIKE 'sparse_yyyyy0.%' OR keystr LIKE 'sparse_yyyyy0[%' order by objid;", "xxxxx",params[0]).replace("yyyyy", params[1])]

    params = query_params[4]
    query5 = ["DROP TABLE IF EXISTS argo_im;",
              "CREATE TEMPORARY TABLE argo_im AS SELECT DISTINCT objid FROM argo_str_tbl WHERE (keystr = 'str1') AND (valstr = '{}');".format(params[0]),
              "SELECT objid, keystr, valstr, CAST (NULL AS DOUBLE PRECISION), CAST (NULL AS BOOLEAN) FROM argo_str_tbl WHERE objid IN (SELECT objid FROM argo_im) UNION SELECT objid, keystr, CAST (NULL AS TEXT), valnum, CAST (NULL AS BOOLEAN) FROM argo_num_tbl WHERE objid IN (SELECT objid FROM argo_im) UNION SELECT objid, keystr, CAST (NULL AS TEXT), CAST (NULL AS DOUBLE PRECISION), valbool FROM argo_bool_tbl WHERE objid IN (SELECT objid FROM argo_im) order by objid;"]

    params = query_params[5]
    query6 = ["DROP TABLE IF EXISTS argo_im;",            
              "CREATE TEMPORARY TABLE argo_im AS SELECT DISTINCT objid FROM argo_num_tbl WHERE (keystr = 'num') AND (valnum >= {} and valnum <= {});".format(params[0], params[1]),
              "SELECT objid, keystr, valstr, CAST (NULL AS DOUBLE PRECISION), CAST (NULL AS BOOLEAN) FROM argo_str_tbl WHERE objid IN (SELECT objid FROM argo_im) UNION SELECT objid, keystr, CAST (NULL AS TEXT), valnum, CAST (NULL AS BOOLEAN) FROM argo_num_tbl WHERE objid IN (SELECT objid FROM argo_im) UNION SELECT objid, keystr, CAST (NULL AS TEXT), CAST (NULL AS DOUBLE PRECISION), valbool FROM argo_bool_tbl WHERE objid IN (SELECT objid FROM argo_im) order by objid;"]

    params = query_params[6]
    query7 = ["DROP TABLE IF EXISTS argo_im;",
              "CREATE TEMPORARY TABLE argo_im AS SELECT DISTINCT objid FROM argo_num_tbl WHERE (keystr = 'dyn1') AND (valnum >= {} and valnum <= {});".format(params[0], params[1]),
              "SELECT objid, keystr, valstr, CAST (NULL AS DOUBLE PRECISION), CAST (NULL AS BOOLEAN) FROM argo_str_tbl WHERE objid IN (SELECT objid FROM argo_im) UNION SELECT objid, keystr, CAST (NULL AS TEXT), valnum, CAST (NULL AS BOOLEAN) FROM argo_num_tbl WHERE objid IN (SELECT objid FROM argo_im) UNION SELECT objid, keystr, CAST (NULL AS TEXT), CAST (NULL AS DOUBLE PRECISION), valbool FROM argo_bool_tbl WHERE objid IN (SELECT objid FROM argo_im) order by objid;"]

    params = query_params[7]
    query8 = ["DROP TABLE IF EXISTS argo_im;",
              "CREATE TEMPORARY TABLE argo_im AS SELECT DISTINCT objid FROM argo_str_tbl WHERE (keystr like 'nested_arr[%]') AND (valstr = '{}');".format(params[0]),
              "SELECT objid, keystr, valstr, CAST (NULL AS DOUBLE PRECISION), CAST (NULL AS BOOLEAN) FROM argo_str_tbl WHERE objid IN (SELECT objid FROM argo_im) UNION SELECT objid, keystr, CAST (NULL AS TEXT), valnum, CAST (NULL AS BOOLEAN) FROM argo_num_tbl WHERE objid IN (SELECT objid FROM argo_im) UNION SELECT objid, keystr, CAST (NULL AS TEXT), CAST (NULL AS DOUBLE PRECISION), valbool FROM argo_bool_tbl WHERE objid IN (SELECT objid FROM argo_im) ORDER BY objid;"]

    params = query_params[8]
    query9 = ["DROP TABLE IF EXISTS argo_im;",
              "CREATE TEMPORARY TABLE argo_im AS SELECT DISTINCT objid FROM argo_str_tbl WHERE (keystr = 'sparse_xxxxx') AND (valstr = '{}');".format(params[1]).replace("xxxxx", params[0]),
              "SELECT objid, keystr, valstr, CAST (NULL AS DOUBLE PRECISION), CAST (NULL AS BOOLEAN) FROM argo_str_tbl  WHERE objid IN (SELECT objid FROM argo_im) UNION SELECT objid, keystr, CAST (NULL AS TEXT), valnum, CAST (NULL AS BOOLEAN) FROM argo_num_tbl WHERE objid IN (SELECT objid FROM argo_im) UNION SELECT objid, keystr, CAST (NULL AS TEXT), CAST (NULL AS DOUBLE PRECISION), valbool FROM argo_bool_tbl WHERE objid IN (SELECT objid FROM argo_im) ORDER BY objid;"]

    params = query_params[9]
    query10 = ["DROP TABLE IF EXISTS argo_im;",
               "CREATE TEMPORARY TABLE argo_im AS SELECT objid FROM argo_num_tbl WHERE (keystr = 'num') and (valnum >= {} and valnum <= {})".format(params[0], params[1]),
               "SELECT valnum as thousandth, count(*) FROM argo_num_tbl WHERE objid in (SELECT objid FROM argo_im) AND keystr = 'thousandth' GROUP BY valnum"]

    params = query_params[10]
    query11 = ["DROP TABLE IF EXISTS argo_im;",
               "CREATE TEMPORARY TABLE argo_im AS SELECT objid FROM argo_num_tbl WHERE (keystr = 'num') and (valnum >= {} and valnum <= {})".format(params[0], params[1]),
               "SELECT * FROM argo_str_tbl AS argo_join_left, argo_str_tbl AS argo_join_right WHERE argo_join_left.keystr = 'nested_obj.str' AND argo_join_right.keystr = 'str1' AND argo_join_left.valstr = argo_join_right.valstr AND argo_join_left.objid in (SELECT objid FROM argo_im);"]

    queries = [query1, query2, query3, query4, query5, query6, query7, query8, query9, query10, query11]
    for indx in range(len(queries)):
        query = queries[indx]
        queries[indx] = replace_table_names(query)


    query_time = ['postgres']
    for indx in range(len(queries)):
        query_list = queries[indx]
        start = time.time() 
        #output_file = 'postgres_query' + str(indx+1) + '.json'
        #fp = open(output_file, 'w')
        for query in query_list:
            res = None
            count = 0
            #print 'executing:', query, '\n'
            select = query[0:6] == 'SELECT'
            if indx < 9 or indx == 10:
                res = process_sql(query, select)
            else:
                res = process_agg_sql(query, select)
            if res is None:
                continue
            for row in res:
                #fp.write(json.dumps(row, sort_keys=True))
                #fp.write('\n')
                count += 1
        end = time.time()
        exec_time = round(end-start, 6)
        print 'query', (indx+1), ' time:', exec_time, 'count: ', count
        query_time.append(exec_time)
        #fp.close()
    print query_time
    return query_time


if __name__ == "__main__":
    main(sys.argv[1:])
