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

    app_conf = init.INIT_APP()
    cre = 1
    pump = 1
    options = None
    idx_opt= None
    trig_opt = None
    rc = 1
    if rc != 0:
        try:
            t1 = time.time()
            #tables
            seqs = db_create.get_triggers_gen_info()
            if cre == 1:
                options = db_create.get_fb_tables()
            if options:
                for sql in db_create.cre_sql_tables(options, seqs):
                    db_create.set_exec(sql, debug=1)
            #indices
            if cre == 1:
                idx_opt = db_create.get_indices_info()
            if idx_opt:
                for sql in db_create.cre_sql_indices(idx_opt):
                    db_create.set_exec(sql, debug=1)

            #main pump
            if pump == 1:
                for t, f in db_create.get_pg_tables():
                    db_create.get_fb_data(t, f, debug=1)

            if pump == 1:
                for sql in db_create.get_generator_value():
                    db_create.set_exec(sql, debug=1)

            #triggers
            if cre == 1:
                trig_opt = db_create.get_triggers_info()
            if trig_opt:
                for sql in db_create.cre_sql_trigers(trig_opt):
                    db_create.set_exec(sql, debug=1)


            t2 = time.time()
            dt = t2 - t1
            print(time.strftime("Total executed time: %H:%M:%S", time.gmtime(dt)), flush=True)

            return 0
            
            #check results
            sql = """select id_spr, c_tovar, c_opisanie, dt, dt_actual from SPR where id_spr = 36"""
            res = db_create.pg_check(sql)
            for row in res:
                for col in row:
                    try:
                        col = col.tobytes()
                    except:
                        pass
                    try:
                        col = col.decode()
                    except:
                        pass
                    print(col, sep='\t', end='\t')
                print()

            while True:
                time.sleep(1)
                break
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


"""
пример запроса с ограничением строк для pg
select id, "USER", params from users order by id limit 4 offset 0;
"""
