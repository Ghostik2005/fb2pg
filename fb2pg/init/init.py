#coding: utf-8
#init first

import os
import time
import queue
import shutil
import sqlite3
import zipfile
import argparse
import threading
import traceback
import configparser
import urllib.parse




class INIT_APP(object):

    def __init__(self):
        super().__init__()


    def get_tables(self):
        pass




def shutdown():
    print('at exit', flush=True)
