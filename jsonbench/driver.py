import csv
import sys
import quickstep_driver
import postgres_driver
import mongo_driver
import pickle
import nobench_gendata
import json2bulksql
import os
import math
import random
import subprocess
import psycopg2
import argparse
from pymongo import MongoClient
from random import randint
import Global

mongo_client = MongoClient()
mongo_db = mongo_client.argocompdb
input_filename = 'nobench_data_argo'
extra_input_filename = input_filename + '_extra'
recstringfile = 'rec_strs'
benchmark_out_file = 'results'+ str(Global.datasize) +'.csv'
params = []
filename = ''
extra_filename = ''
outfile = None
data = mongo_db[Global.collection_name]

class OutFileHandler:
    def __init__(self, filename):
        self.filename = filename

    def write_headers(self):
        with open(self.filename, 'w') as csvfile:
            writer = csv.writer(csvfile, delimiter=',',)
            writer.writerow(['System', 'Q1', 'Q2', 'Q3', 'Q4', 'Q5', 'Q6', 'Q7', 'Q8', 'Q9', 'Q10', 'Q11', 'Q12'])

    def write_row(self, results_list):
        with open(self.filename, 'a') as csvfile:
            for results in results_list:
                writer = csv.writer(csvfile, delimiter=',',)
                writer.writerow(results)


def generate_data():
    recommended_strings = nobench_gendata.main_non_cli(Global.datasize, False, input_filename, extra_input_filename)
    with open(recstringfile, 'wb') as outfile:
        pickle.dump(recommended_strings, outfile)
    json2bulksql.convertFile(input_filename, Global.datasize, 0, True, False)
    json2bulksql.convertFile(extra_input_filename, Global.datasize*0.001, Global.datasize, True, False)


def configure_test_run():
    Global.datasize = 1000
    global input_filename
    input_filename = input_filename + '_test'
    global extra_input_filename
    extra_input_filename = input_filename + '_extra'
    global recstringfile
    recstringfile = recstringfile + '_test'
    Global.collection_name = Global.collection_name + '_test'
    global data
    data = mongo_db[Global.collection_name]
    global benchmark_out_file
    benchmark_out_file = 'results_test'+ str(Global.datasize) +'.csv'
    
    Global.argo_str_tbl = 'argo_' + Global.collection_name + '_str'
    Global.argo_num_tbl = 'argo_' + Global.collection_name + '_num'
    Global.argo_bool_tbl = 'argo_' + Global.collection_name + '_bool'


def gen_query3_params():
    rand_digit = 1 + randint(0,8)  
    digit_str = str(rand_digit) + str(rand_digit)
    return [digit_str]

def gen_query4_params():
    rand_digit1 = 1 + randint(0,8)
    rand_digit2 = 1 + randint(0,8)
    while(rand_digit2 == rand_digit1):
        rand_digit2 = 1 + randint(0,8)

    digit1_str = str(rand_digit1) + str(rand_digit1)
    digit2_str = str(rand_digit2) + str(rand_digit2)
    return [digit1_str, digit2_str]

def gen_query5_params():
    halfway_index = Global.datasize / 2
    results = data.find({}, {"str1": 1, "_id": 0})
    params = []
    for index, result in enumerate(results):
        if index == halfway_index:
            params.append(str(result['str1']))
    return params

def gen_query6_params():
    data_slice_size = math.ceil(Global.datasize * 0.001)
    rand_num = randint(1, Global.datasize)
    params = []
    #Changing the parameters of the query based on the trial size.
    params.append(str(rand_num))
    params.append(str(int(rand_num + data_slice_size)))
    return params

def gen_query7_params():
    params = []
    data_slice_size = math.ceil(Global.datasize * 0.001)
    rand_num = randint(1, Global.datasize)
    params.append(str(rand_num))
    params.append(str(int(rand_num + data_slice_size)))
    return params

def gen_query8_params():
    rec_strs = []
    with open(recstringfile, "rb") as infile:
        rec_strs = pickle.load(infile)
    random.shuffle(rec_strs)
    return [str(rec_strs[0])]

def gen_query9_params():
    params = []
    rand_digit = 1 + randint(0, 8)
    digit_str = str(rand_digit) + str(rand_digit) + str(rand_digit)
    sparse_str = "sparse_" + digit_str
    params.append(digit_str)
    results = data.find({}, {sparse_str: 1, "_id": 0})
    for index, result in enumerate(results):
        try:
            params.append(str(result[sparse_str]))
        except KeyError:
            continue
        else:
            break
    return params

def gen_query10_params():
    params = []
    data_slice_size = math.ceil(Global.datasize * 0.001)
    rand_num = randint(1, Global.datasize)
    params.append(str(rand_num))
    params.append(str(int(rand_num + data_slice_size)))
    return params

def gen_query11_params():
    params = []
    data_slice_size = math.ceil(Global.datasize * 0.001)
    rand_num = randint(1, Global.datasize)
    params.append(str(rand_num))
    params.append(str(int(rand_num + data_slice_size)))
    return params

def gen_query_params(query_id):
    if query_id <= 2:
        return []
    elif query_id == 3:
        return gen_query3_params()
    elif query_id == 4:
        return gen_query4_params()
    elif query_id == 5:
        return gen_query5_params()
    elif query_id == 6: 
        return gen_query6_params()
    elif query_id == 7:
        return gen_query7_params()
    elif query_id == 8:
        return gen_query8_params()
    elif query_id == 9:
        return gen_query9_params()
    elif query_id == 10:
        return gen_query10_params()
    elif query_id == 11:
        return gen_query11_params()

def gen_params():
    global params
    num_queries = 11
    for query_id in range(num_queries):
        query_params = gen_query_params(query_id + 1)
        params.append(query_params)
    print params

def get_driver(system):
    if system == 'mongo':
        return mongo_driver
    elif system == 'psql':
        return postgres_driver
    elif system == 'qstep':
        return quickstep_driver

def exec_action(system, action):
    driver = get_driver(system) 
    print 'Performing %s on %s' %(action, system)
    if action == 'load':
        print 'Loading %s data' %(system)
        time = driver.load_data(filename)
        print 'Load Time:', time
    elif action == 'run':
        print "Executing %s Test Cases" %(system)
        results_list = []
        for i in range(Global.runs):
            results_list.append(driver.main(params))
        for i in range(Global.runs):
            exec_time = driver.load_data(extra_filename, False)
            results_list[i].append(exec_time)
        outfile.write_row(results_list)
    elif action == 'clean':
        driver.clean_db()

def main(argv):
    parser = argparse.ArgumentParser()
    # optional flags to run list of user given actions on user given systems
    # you cant perform different actions on different systems; all systems same actions

    parser.add_argument("--gen", help="generate data", action="store_true")
    parser.add_argument("--load", help="load data", action="store_true")
    parser.add_argument("--run", help="execute nobench", action="store_true")
    parser.add_argument("--psql", help="nobench for psql", action="store_true")
    parser.add_argument("--mongo", help="nobench for mongo", action="store_true")
    parser.add_argument("--qstep", help="nobench for qstep", action="store_true")
    parser.add_argument("--test", help="nobench on test data", action="store_true")
    parser.add_argument("--noclean", help="do not clean data on systems", action="store_true")
    args = parser.parse_args()
    systems = ['mongo', 'psql', 'qstep']
    actions = ['gen', 'load', 'run', 'clean'] 
    env = 'prod'
    user_systems = []
    user_actions = []
    if args.psql:
        user_systems.append('psql')
    if args.mongo:
        user_systems.append('mongo')
    if args.qstep:
        user_systems.append('qstep')
    if args.gen:
        user_actions.append('gen')
    if args.load:
        user_actions.append('load')
    if args.run:
        user_actions.append('run')
    if args.noclean:
        actions.remove('clean')
    if args.test:
        env = 'test'

    if len(user_systems) == 0:
        # add all systems by default
        user_systems.extend(systems)
    if len(user_actions) == 0:
        # add all actions by default
        user_actions.extend(actions)
   
    if env == 'test':
        configure_test_run()

    random.seed()

    # write header once
    global outfile
    outfile = OutFileHandler(benchmark_out_file)
    outfile.write_headers()
    # gen data only once
    if 'gen' in user_actions:
        generate_data()
    # gen params only once
    DIR = os.getcwd()
    global filename
    filename = DIR + '/' + input_filename
    global extra_filename
    extra_filename = DIR + '/' + extra_input_filename

    for action in user_actions:
        if action == 'gen':
            continue
        if action == 'run' and len(params) == 0:
            gen_params()
        for system in user_systems:
            exec_action(system, action)
   

if __name__ == "__main__":
        main(sys.argv[1:])
