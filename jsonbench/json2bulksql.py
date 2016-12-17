#!/usr/bin/env python

import sys
import string
import optparse

sys.path.append("third_party")
import ijson
import progressbar

try:
    import json
except ImportError:
    import simplejson as json


class BulkSQLFileWriterObjectHandler(object):
    def __init__(self, outfile_str, outfile_num, outfile_bool):
        self.outfile_str = outfile_str
        self.outfile_num = outfile_num
        self.outfile_bool = outfile_bool

    def handleInternal(self, objid, key, obj):
        if isinstance(obj, basestring):
            print >> self.outfile_str, str(objid) + "|" + key + "|" + obj
        elif isinstance(obj, bool):
            if (obj):
                print >> self.outfile_bool, str(objid) + "|" + key + "|1"
            else:
                print >> self.outfile_bool, str(objid) + "|" + key + "|0"
        elif isinstance(obj, int) or isinstance(obj, float):
            print >> self.outfile_num, str(objid) + "|" + key + "|" + str(obj)
        elif isinstance(obj, list):
            for (idx, list_item) in enumerate(obj):
                self.handleInternal(objid, key + "[" + str(idx) +"]", list_item)
        elif isinstance(obj, dict):
            if (len(key) > 0):
                prekey = key + "."
            else:
                prekey = ""
            for (subkey, subval) in obj.iteritems():
                self.handleInternal(objid, prekey + subkey, subval)

    def handle(self, obj, objid, last=False):
        self.handleInternal(objid, "", obj)


class SingleTableBulkSQLFileWriterObjectHandler(object):
    def __init__(self, outfile):
        self.outfile = outfile

    def handleInternal(self, objid, key, obj):
        if isinstance(obj, basestring):
            print >> self.outfile, str(objid) + "|" + key + "|" + obj + "|\\N|\\N"
        elif isinstance(obj, bool):
            if (obj):
                print >> self.outfile, str(objid) + "|" + key + "|\\N|\\N|1"
            else:
                print >> self.outfile, str(objid) + "|" + key + "|\\N|\\N|0"
        elif isinstance(obj, int) or isinstance(obj, float):
            print >> self.outfile, str(objid) + "|" + key + "|\\N|" + str(obj) + "|\\N"
        elif isinstance(obj, list):
            for (idx, list_item) in enumerate(obj):
                self.handleInternal(objid, key + "[" + str(idx)+"]", list_item)
        elif isinstance(obj, dict):
            if (len(key) > 0):
                prekey = key + "."
            else:
                prekey = ""
            for (subkey, subval) in obj.iteritems():
                self.handleInternal(objid, prekey + subkey, subval)

    def handle(self, obj, objid, last=False):
        self.handleInternal(objid, "", obj)


def convertFile(input_filename, datasize, start_objid=0, show_pbar=False, single_table=False):
    infile = open(input_filename, 'r')
    #collection = json.load(infile)

    basename = string.rsplit(input_filename, ".", 1)[0]
    if (single_table):
        outfile = open(basename + ".txt", 'w')
    else:
        outfile_str = open(basename + "_str.txt", 'w')
        outfile_num = open(basename + "_num.txt", 'w')
        outfile_bool = open(basename + "_bool.txt", 'w')

    if (show_pbar):
        widgets = ['Writing Data Files: ', progressbar.Percentage(), ' ', progressbar.Bar(), ' ', progressbar.ETA()]
        pbar = progressbar.ProgressBar(widgets=widgets, maxval=datasize).start()

    if (single_table):
        writer = SingleTableBulkSQLFileWriterObjectHandler(outfile)
    else:
        writer = BulkSQLFileWriterObjectHandler(outfile_str, outfile_num, outfile_bool)

    #for (objid, obj) in enumerate(collection):
    #    print str(objid) + "---" + str(obj)
    #    writer.handle(obj, objid + start_objid)
    #    if (show_pbar):
    #        pbar.update(objid + 1)

    infile.seek(0)
    parser = ijson.items(infile, "item")
    for (objid, item) in enumerate(parser):
        writer.handle(item, objid + start_objid)
        if show_pbar:
            pbar.update(objid + 1)



    if (show_pbar):
        pbar.finish()

    infile.close()
    if (single_table):
        outfile.close()
    else:
        outfile_str.close()
        outfile_num.close()
        outfile_bool.close()


def main(filename="nobench_data.json"):
    cli_parser = optparse.OptionParser(usage="usage: %prog [options] input.json")
    cli_parser.add_option("-n", "--start-objid", action="store", type="int", dest="start_objid", default=0,
                          metavar="NUMBER", help="start numbering object IDs from NUMBER")
    cli_parser.add_option("-s", "--single-table", action="store_true", dest="single_table", default=False,
                          help="Output a single file for the single 5-column table format")

    (options, args) = cli_parser.parse_args()
    #TODO THIS IS MY MOD
    print args
    if len(args) == 0:
        print "appending filename" + filename
        args.append(filename)
    if (len(args) != 1):
        print "You did not supply exactly 1 json file to convert"
        sys.exit(1)

    convertFile(args[0], options.start_objid, True, options.single_table)


if __name__ == "__main__":
    main()
