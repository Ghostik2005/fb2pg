#coding: utf-8
import sys
import time
import threading
import traceback

import init
import db_create

def main():
    if sys.stdout.encoding != 'UTF-8':
        sys.stdout = open(sys.stdout.fileno(), mode='w', buffering=1, encoding='UTF-8')
    if sys.stderr.encoding != 'UTF-8':
        sys.stderr = open(sys.stderr.fileno(), mode='w', buffering=1, encoding='UTF-8')
    ini = init.INIT_APP()
    cre = ini.params.create
    rc = 0
    if cre:
        pump = ini.params.pump
        options = None
        idx_opt= None
        trig_opt = None
        try:
            t1 = time.time()
            #tables
            seqs = db_create.get_triggers_gen_info(ini)
            if cre == 1:
                options = db_create.get_fb_tables(ini)
            if options:
                for sql in db_create.cre_sql_tables(ini, options, seqs):
                    db_create.set_exec(ini, sql, debug=1)
            #indices
            if cre == 1:
                idx_opt = db_create.get_indices_info(ini)
            if idx_opt:
                for sql in db_create.cre_sql_indices(ini, idx_opt):
                    pass
                    db_create.set_exec(ini, sql, debug=1)

            #main pump
            if pump == 1:
                for t, f in db_create.get_pg_tables(ini):
                    db_create.get_fb_data(ini, t, f, only=ini.params.only, debug=1)

            if pump == 1:
                for sql in db_create.get_generator_value(ini):
                    db_create.set_exec(ini, sql, debug=1)

            #triggers
            if cre == 1:
                trig_opt = db_create.get_triggers_info(ini)
            if trig_opt:
                for sql in db_create.cre_sql_trigers(ini, trig_opt):
                    db_create.set_exec(ini, sql, debug=1)

            t2 = time.time()
            dt = t2 - t1
            print(time.strftime("Total executed time: %H:%M:%S", time.gmtime(dt)), flush=True)

        except KeyboardInterrupt as e:
            pass
        except SystemExit as e:
            if e:
                rc = e.code
        except:
            print("COMMON_ERROR: %s" % traceback.format_exc(), flush=True)
        finally:
            init.shutdown()
    else:
        print("INITERROR: Ошибка инициализации", flush=True)
    return rc

if "__main__" == __name__:
    sys.exit(main())

