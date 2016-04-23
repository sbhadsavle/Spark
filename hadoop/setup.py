#!/usr/bin/env python
import yaml, sys, os, shutil
from glob import glob
from subprocess import call

# setup.py setups configures namenodes and datanodes for Hadoop.

#
# Vars
# 

VERSION  = "0.1.0"
HELP     = """Hadoop setup on AWS Ubuntu 14.04.\n
Usage:
    setup.py (namenode|datanode) <addresses.yaml>
    setup.py -h | --help
    setup.py --version"""

# URLs
HADOOP_DISTRIBUTION_URL      = "https://storage.googleapis.com/ee360p-files/hadoop/hadoop-{}.tar.gz"
HADOOP_DISTRIBUTION_FILENAME = "hadoop-{}.tar.gz"

# Environmental variables
HOME            = os.environ["HOME"]
DOWNLOADS       = HOME+"/Downloads"
USER            = os.environ["USER"]
HADOOP_HOME     = "/usr/local/hadoop"
HADOOP_CONF_DIR = HADOOP_HOME + "/etc/hadoop"
JAVA_HOME       = "/usr/lib/jvm/java-7-openjdk-amd64/jre"

#
# Helper functions
#

# Parses the YAML file and returns dict
# of the contents.
def parse_yaml(file):
    with open(file) as f:
        m = yaml.load(f)
        f.close()
        return m

# Appends the environmental variables required for Hadoop to ~/.profile
# Does not source the written file after writing.
def write_hadoop_env_vars():
    env = """\n# Hadoop env vars
export JAVA_HOME={}
export PATH=$PATH:$JAVA_HOME/bin
export HADOOP_HOME={}
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_COMMON_HOME=$HADOOP_HOME
export HADOOP_HDFS_HOME=$HADOOP_HOME
export YARN_HOME=$HADOOP_HOME
export PATH=$PATH:$HADOOP_HOME/bin
export HADOOP_CONF_DIR={}""".format(JAVA_HOME, HADOOP_HOME, HADOOP_CONF_DIR)
    with open(HOME+"/.profile", "a") as f:
        f.write(env)
        f.close()

# Replaces old with new for all first-level files in path.
# path can use wildcards.
def replace_pattern(old, new, path):
    call("sed -i -- 's/{}/{}/g' {}".format(old, new, path), shell=True)

# Simple helper for writing string to file.
# Overwrites existing contents. Creates file if file does not exist.
# Takes care of closing file after.
def write_file(file, string):
   with open(file, 'w+') as f:
       f.write(string)
       f.close()

# Insert string at the specified line number
def insert_lines_at(file, string, at):
   with open(file, 'r+') as f:
      lines = f.readlines()
      for i, line in enumerate(lines):
          if i == at:
             lines.insert(at, string)
      f.truncate(0)         # truncates the file
      f.seek(0)             # moves the pointer to the start of the file
      f.writelines(lines)   # write the new data to the file
      f.close()

# Cleans Hadoop paths
# Used when re-running script on existing machine
def clean_hadoop_dirs():
    for f in glob(HOME+'/usr/local/hadoop*'):
        os.remove(f)
    shutil.rmtree('/usr/local/hadoop', ignore_errors=True)

#
# Script
# 

if __name__ == '__main__':
    # parse args
    if len(sys.argv) < 3:
        print(HELP)
        exit(1)

    if sys.argv[1] == "--help" or sys.argv[1] == "-h":
        print(HELP)
        exit(0)

    if sys.argv[1] == "--version":
        print(VERSION)
        exit(0)

    # namenode or datanode
    identity = sys.argv[1]
    if identity != "namenode" and identity != "datanode":
        print(HELP)
        exit(1)

    # update system packages, install java
    call(["sudo", "apt-get", "-y", "update"])
    call(["sudo", "apt-get", "-y", "install", "openjdk-7-jdk"])
    call(["java", "-version"])

    # download hadoop
    HADOOP_DOWNLOAD_PATH = DOWNLOADS+"/"+HADOOP_DISTRIBUTION_FILENAME.format(identity)
    if not os.path.isfile(HADOOP_DOWNLOAD_PATH):
        call(["wget", HADOOP_DISTRIBUTION_URL.format(identity), "-P", DOWNLOADS])
    # clean up existing dirs if any
    clean_hadoop_dirs()
    # extract
    call(["tar", "zxvf", HADOOP_DOWNLOAD_PATH, "-C", "/usr/local"])
    call(["mv", "/usr/local/hadoop-"+identity, "/usr/local/hadoop"])

    # read addresses file
    addresses = parse_yaml(sys.argv[2])
    namenode = addresses['namenode']
    datanodes = addresses['datanodes']

    # setup environmental variables (common)
    write_hadoop_env_vars()
    # replace namenode address (common)
    replace_pattern("namenode_public_dns", namenode['public_dns'], HADOOP_CONF_DIR+"/*")
    # edit /etc/hosts (common)
    address_str = namenode['public_dns']+" "+namenode['private_dns_prefix']+"\n"
    address_str += "".join(map(lambda d: d['public_dns']+" "+d['private_dns_prefix']+"\n", datanodes))
    insert_lines_at("/etc/hosts", address_str, 1)    
    # edit masters and slaves files (common)
    write_file(HADOOP_CONF_DIR+"/masters", namenode['private_dns_prefix']+"\n")
    write_file(HADOOP_CONF_DIR+"/slaves", "".join(map(lambda d: d['private_dns_prefix']+"\n", datanodes)))
    # hdfs site replication (common)
    replace_pattern("dfs_replication_count", str(len(datanodes)), HADOOP_CONF_DIR+"/hdfs-site.xml")
    
    # remove existing hadoop data dirs
    # create data directories for namenode/datanode
    for f in glob(HADOOP_HOME+'/hadoop_data'):
        os.remove(f)
    call(["sudo", "mkdir", "-p", HADOOP_HOME+"/hadoop_data/hdfs/"+identity])
    
    # update permissions (common)
    call(["sudo", "chown", "-R", USER, HADOOP_HOME])
