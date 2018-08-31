#coding: utf-8
#init first

import os
import sys
import time
import configparser
from dataclasses import dataclass

@dataclass
class INIT_PARAMS:
    def set_attr(self, name, value=None):
        self.__setattr__(name, value)
    pass

class INIT_APP(object):

    def __init__(self):
        super().__init__()
        self.params = INIT_PARAMS()
        self.get_ini()

    def get_ini(self):
        config = configparser.ConfigParser()
        config.read('fb2pg.ini', encoding='UTF-8')
        init = config['init']
        fb = config['fb']
        pg = config['pg']
        kf = fb.keys()
        kp = pg.keys()
        fbp = {}
        pgp = {}
        for k in kf:
            fbp[k] = fb.get(k)
        for k in kp:
            pgp[k] = pg.get(k)
        self.params.fb_params = fbp
        self.params.pg_params = pgp
        self.params.pump = init.get('pump')
        if self.params.pg_params['password'].lower().title() == 'None':
            self.params.pg_params.pop('password')
        if self.params.fb_params['password'].lower().title() == 'None':
            self.params.fb_params.pop('password')
        if self.params.fb_params['port']:
            self.params.fb_params['port'] = int(self.params.fb_params['port'])
        if self.params.pg_params['port']:
            self.params.pg_params['port'] = int(self.params.pg_params['port'])
        if self.params.pump == True or self.params.pump.title() == 'True':
            self.params.pump = 1
        elif self.params.pump == False or self.params.pump.title() == 'False':
            self.params.pump = 0
        self.params.create = init.get('create')
        if self.params.create == True or self.params.create.title() == 'True':
            self.params.create = 1
        elif self.params.create == False or self.params.create.title() == 'False':
            self.params.create = 0
        self.params.potion = int(init.get('potion'))
        self.params.only = init.get('only_one')
        excl = init.get('excludes')
        excl = excl.split(',')
        self.params.excludes = excl
        self.params.cpu = self.cpu_count()


    def cpu_count(self):
        res = int(os.sysconf('SC_NPROCESSORS_ONLN'))
        return res

def shutdown():
    print('at exit', flush=True)

