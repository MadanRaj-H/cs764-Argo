#!/usr/bin/python

import os
import os.path as path
import random
import re
import subprocess as sp
import sys

# Config file
QSCONF = ".pyqs_profile"

# Prints info about query
DEBUG = False

_re_rowdelim = re.compile("(\+-+)*\+")
_re_time = re.compile("Time: (\d+\.\d+) ms")
_cli_flags = {}

### Functions

def on_import():
  # Do check to see if we should import CLI flags.
  get_cli_flags()
  setup_default_store()

  if DEBUG:
    global _cli_flags
    print _cli_flags

def setup_default_store():
  """
  Runs quickstep with the initialize_db flag set to true if the program detects
  that the tuple storage directory has not been created.
  """
  global _cli_flags
  if _cli_flags.has_key('storage_path') and \
      not _cli_flags.has_key('initialize_db'):
    qstor = _cli_flags['storage_path']
    if not path.isdir(qstor):
      print 'setting to true'
      # Run quickstep with initialize option to create the db folder.
      proc = sp.Popen([find_qs()] + flags() + ["-initialize_db=true"], stdin=sp.PIPE, stdout=sp.PIPE)
      proc.communicate("quit;")
      

def find_qs():
  """
  Locates the quickstep executable
  """
  # TODO(marc): This could be improved, possibly using the rc file.
  #locations = sp.check_output(["find", "/", "-name", "quickstep_cli_shell"], stderr=os.devnull)
  locations = '/proj/streaming-PG0/764/quickstep/build/quickstep_cli_shell\n'
  exes = locations.split("\n")[:-1]

  if len(locations) == 0:
    raise Exception("unable to find quickstep executable")

  return exes[0]

def process_qs_error(err_msg):
  print "Fatal error:\n{}".format(err_msg)
  raise Exception("Fatal error:\n{}".format(err_msg))

def process_qs_out(qs_out):
  if "ERROR:" in qs_out[0:6]:
    return parse_error(qs_out)
  return parse_table_result(qs_out)

def parse_cli_args(conf_str):
  """
  Parse the lines of the configuration style.
  Config files will be specified as:
    CLI_ARG1 CLI_ARG_VALUE1
    CLI_ARG2 CLI_ARG_VALUE2
    ..
    CLI_ARGN CLI_ARG_VALUEN
  """
  args = {}
  # TODO(marc): add check for valid arguments.
  for line in conf_str.split('\n'):

    # Check for commented line.
    stripped_line = line.strip()
    if len(stripped_line) == 0 or stripped_line[0] == '#':
      continue

    try:
      kv = line.split()
      if len(kv) == 0:
        # empty line
        continue
      elif len(kv) != 2:
        raise Exception()
      args[kv[0]] = kv[1]
    except:
      print("bad rc file: {}".format(line))
      return -1

  return args

def get_cli_flags():
  """
  Gets flags defined in the config file or uses the default flags if
  the file does not exist.
  """
  # Check if the config file exists.
  conf_str = ""
  if path.isfile(QSCONF):
    try:
      with open(QSCONF, 'r') as conf_file:
        conf_str = conf_file.read()
    except IOError:
      print "failed to open {}".format(QSCONF)

  if conf_str:
    args = parse_cli_args(conf_str)
    if args == -1:
      print "bad rc file, using default"
    else:
      flags = args
  else:
    flags = get_default_cli_flags()

  # set global state.
  global _cli_flags
  _cli_flags = flags
  return _cli_flags

def get_default_cli_flags():
  """
  Returns reasonable default command line flags.
  """
  return {"storage_path" : "/proj/streaming-PG0/764/quickstep/build/qsstor"}

def flags():
  """
  Returns an array of string of command line flags.
  """
  global _cli_flags
  if _cli_flags == {}:
    _cli_flags = get_cli_flags()

  argv = []
  for k,v in _cli_flags.iteritems():
    argv.append("-" + k + "=" + v)
  return argv

def query(qstr): 
  qs_exe = find_qs()
  qstr += ";" if qstr[-1] != ';' else ''

  if DEBUG:
    print("{} < '{}'".format(" ".join([qs_exe] + flags()), qstr))

  qs = sp.Popen([qs_exe] + flags(), stdin=sp.PIPE, stdout=sp.PIPE, stderr=sp.PIPE)
  qout, qerr = qs.communicate(qstr)
  # An error indicates that quickstep hard crashed.

  if qerr and "WARNING:" in qerr[0:8]:
        return process_qs_out(qout)
  elif qerr:
  	return process_qs_error(qerr)
  else:
        return process_qs_out(qout)
	
def parse_table_result(qs_out):
  """
  Parses the output from a single query and returns a tuple of column header
  and then array of tuples constituting the parsed table.
  """
  col_widths = []
  head = []
  values = []
  meta = {}
  STATE = 0
  for line in qs_out.split('\n'):
    if STATE == 0:
      # Searching for the head of the result.
      # If we never find the head, that means we are in a DML query so we don't
      # parse the resulting table.
      if _re_rowdelim.match(line):
        length = 0
        for c in line[1:]:
          if c == '+':
            col_widths.append(length)
            length = 0
          else:
            length += 1
          STATE = 1
      elif _re_time.match(line):
        # TODO(marc): code repetition.
        exe_time = _re_time.findall(line)
        if len(exe_time) == 1:
          meta['time_ms'] = float(exe_time[0])
          break
        
    elif STATE == 1:
      # Parse the head.
      start = 1
      for length in col_widths:
        head.append(line[start : start + length].strip())
        start += length + 1
      STATE = 2

    elif STATE == 2:
      # Parse the line after the head.
      if not _re_rowdelim.match(line):
        print("malformed query results")
      else:
        STATE = 3

    elif STATE == 3:
      # End of query results.
      if _re_rowdelim.match(line):
        STATE = 4
        continue
      # Else, add line to values.
      tuple_vals = []
      start = 1
      for length in col_widths:
        tuple_vals.append(line[start : start + length].strip())
        start += length + 1
      values.append(tuple_vals)

    elif STATE == 4:
      # Look for the execution time.
      exe_time = _re_time.findall(line)
      if len(exe_time) == 1:
        meta['time_ms'] = float(exe_time[0])
        break

  return head, values, meta

def parse_error(qs_out):
  """
  Raises the error string as an exception.
  """
  raise Exception(qs_out)

# Run an assessment of the system when the module is imported.
on_import()
