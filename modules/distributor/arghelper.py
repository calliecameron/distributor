# Helper functions for the 'type' argument of argparse

import argparse
import os.path

def existingFile(s):
    s = os.path.realpath(s)
    if not os.path.exists(s):
        raise argparse.ArgumentTypeError("file '%s' does not exist" % s)
    return s

def existingDir(s):
    s = os.path.realpath(s)
    if not os.path.isdir(s):
        raise argparse.ArgumentTypeError("directory '%s' does not exist" % s)
    return s
