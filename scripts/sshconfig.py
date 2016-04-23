#!/usr/bin/env python

import yaml, os, sys

HELP = """SSH config generator

Usage:
    sshconfig.py <addresses.yaml> <user> <path-to-pem>
    sshconfig.py --help | -h

Example:
    sshconfig.py addresses.yaml ubuntu '~/.ssh/aws-hadoop.pem'"""

class Config:
    '''
    SSH config entry
    '''

    def __init__(self, host, host_name, user, identity_file):
        self.host = host
        self.host_name = host_name
        self.user = user
        self.identity_file = identity_file

    def __str__(self):
        return """Host {}
    HostName {}
    User {}
    IdentityFile {}""".format(
        self.host, self.host_name, self.user, self.identity_file)

    def __repr__(self):
        return self.__str__()

if __name__ == '__main__':
    # parse args
    if len(sys.argv) < 4 or sys.argv[1] == "-h" or sys.argv[1] == "--help":
        print(HELP)
        exit(1)

    file = sys.argv[1]
    user = sys.argv[2]
    keypath = sys.argv[3]

    # parse address file
    m = {}
    with open(file) as f:
        m = yaml.load(f)
        f.close()

    # generate output to stdout
    namenode = m['namenode']
    datanodes = m['datanodes']
    configs = list()
    configs.append(Config(namenode['name'], namenode['public_dns'], user, keypath))
    configs.extend([Config(d['name'], d['public_dns'], user, keypath) for d in datanodes])

    for c in configs:
        print("{}\n".format(c))
