import sys
import os


def set_hadoop_home_dir(path):
    os.environ['HADOOP_HOME'] = path

def is_os_windows():
    return os.name == 'nt'
