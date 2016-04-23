#!/usr/bin/env python

import sys
from subprocess import call

HELP = """
scp multiple files to multiple hosts.

Usage:
    scp.py '<to-dir>' <file>... -- <host>...
    scp.py --help | -h
"""

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print(HELP)
        exit(1)

    if sys.argv[1] == "--help" or sys.argv[1] == "-h":
        print(HELP)
        exit(0)

    index = 0
    try:
        index = sys.argv.index("--")
    except ValueError:
        print(HELP)
        exit(1)

    to_dir, files, hosts = sys.argv[1], sys.argv[2:index], sys.argv[index+1:]
    print("files: {}".format(files))

    for h in hosts:
        print("writing to {} on {}".format(to_dir, h))
        call("scp {} {}:{}".format(" ".join(files), h, to_dir), shell=True)
