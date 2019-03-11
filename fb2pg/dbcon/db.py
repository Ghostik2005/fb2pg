#coding: utf-8
#db connector

import fdb
import sys
import psycopg2
import traceback

class FBConn(object):

    def __init__(self, ini, log=print, debug=False):
        self.log = log
        self.debug = debug
        self.con = None
        self.result = None
        self.error = None
        self.sql = None
        self.connect_params = ini.params.fb_params

    def __enter__(self):
        return self.connect()
        
    def __exit__(self, *args):
        self.close()

    def connect(self):
        #make fb connection
        self.con = fdb.connect(**self.connect_params)
        return self

    def close(self):
        if self.con:
            self.con.close()

    def execute(self, sql):
        self.sql = sql
        self._request(commit=True)
        self.sql = None

    def request(self, sql):
        self.sql = sql
        self._request(commit=False)
        self.sql = None

    def _log(self):
        #self.log("SQLRequest: %s" %self.sql)
        if self.error:
            self.log("SQLError: %s" %self.error)
            self.log('********')
            self.log("SQLRequest: %s" %self.sql)
            self.log('********')
        if self.result:
            pass
            #self.log("SQLResult: %s" %self.result)

    def _request(self, commit=True):
        self.result = None
        self.error = None
        cur = self.con.cursor()
        try:
            cur.execute(self.sql)
        except:
            self.error = traceback.format_exc()
        else:
            if commit:
                self.con.commit()
            try:
                ress = cur.fetchall()
                # print(result)
                ret = []
                for row in ress:
                    ret_row = []
                    for col in list(row):
                        if 'BlobReader' in str(type(col)):
                            # print(dir(col))
                            # print("&"*30)
                            # print(col)
                            ret_row.append(col.read())
                        else:
                            ret_row.append(col)
                    ret.append(ret_row)
                self.result = ret
            except:
                pass
        finally:
            if cur:
                cur.close()
        if self.debug:
            self._log()

    def fetch(self):
        return self.result



class PGConn(object):

    def __init__(self, ini, log=print, debug=False):
        self.log = log
        self.debug = debug
        self.con = None
        self.result = None
        self.error = None
        self.sql = None
        self.params = ini.params.pg_params
        #self.params = {'dbname': self.dbname, 'user': 'postgres', 'host': 'localhost', 'port': 5432}

    def __enter__(self):
        return self.connect()
        
    def __exit__(self, *args):
        self.close()

    def connect(self):
        #make PG connection
        self.con = psycopg2.connect(**self.params)
        return self

    def close(self):
        if self.con:
            self.con.close()

    def executemany(self, sql, params):
        self.sql = sql
        self.params = params
        self.result = None
        self.error = None
        cur = self.con.cursor()
        try:
            cur.executemany(self.sql, self.params)
        except:
            self.error = traceback.format_exc()
            # print("@"*10)
            # print(self.sql)
            # print("@"*10)
            # print(self.params)
            # print("@"*10)
        else:
            self.con.commit()
        finally:
            if cur:
                cur.close()
        if self.debug:
            self._log()

    def execute(self, sql):
        self.sql = sql
        self._request(commit=True)
        self.sql = None

    def request(self, sql):
        self.sql = sql
        self._request(commit=False)
        self.sql = None

    def _log(self):
        #self.log("SQLRequest: %s" %self.sql)
        if self.error:
            self.log('***********')
            self.log("SQLError: %s" %self.error)
            self.log('***********')
            self.log("SQLRequest: %s" %self.sql)
            self.log('***********')
            self.log("SQLParamsResult: %s" %self.params)
            self.log('***********')
        if self.result:
            pass
            #self.log("SQLResult: %s" %self.result)
        if self.params:
            pass
            #self.log("SQLParamsResult: %s" %self.params)

    def _request(self, commit=True):
        self.result = None
        self.error = None
        cur = self.con.cursor()
        try:
            cur.execute(self.sql)
        except:
            self.error = traceback.format_exc()
        else:
            if commit:
                self.con.commit()
            try:
                self.result = cur.fetchall()
            except:
                pass
        finally:
            if cur:
                cur.close()
        if self.debug:
            self._log()

    def fetch(self):
        return self.result


