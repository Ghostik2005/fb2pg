#coding: utf-8

import psycopg2
from multiprocessing.dummy import Pool as ThreadPool
from dbcon import FBConn
from dbcon import PGConn

def get_fb_tables(ini, debug=True):
    sql = """select R.RDB$RELATION_NAME, R.RDB$FIELD_POSITION, R.RDB$FIELD_NAME,
F.RDB$FIELD_LENGTH, 
case f.rdb$field_type
        when 7 then 'smallint'
        when 8 then 'integer'
        when 10 then 'float'
        when 14 then 'char'
        when 16 then -- только диалект 3
          case f.rdb$field_sub_type
            when 0 then 'bigint'
            when 1 then 'numeric'
            when 2 then 'decimal'
            else 'unknown'
          end
        when 12 then 'date'
        when 13 then 'time'
        when 27 then -- только диалект 1
          case f.rdb$field_scale
            when 0 then 'double precision'
            else 'numeric'
          end
        when 35 then 'timestamp'  --или date в зависимости от диалекта
        when 37 then 'varchar'
        when 261 then 'blob'
        else 'unknown'
    end, 
F.RDB$FIELD_SCALE, F.RDB$FIELD_SUB_TYPE,
CASE tt
    WHEN 'PRIMARY KEY' then '1'
    else '0'
END as PK,
CASE r.RDB$NULL_FLAG
    WHEN 1 then '1'
    ELSE '0'
END as notnull,
r.RDB$DEFAULT_SOURCE as def
from RDB$FIELDS F, RDB$RELATION_FIELDS R
left JOIN (
    SELECT c.RDB$RELATION_NAME rn, c.RDB$CONSTRAINT_NAME, seg.RDB$FIELD_NAME jn, c.RDB$CONSTRAINT_TYPE tt
    FROM RDB$RELATION_CONSTRAINTS c
    JOIN RDB$INDEX_SEGMENTS seg on seg.RDB$INDEX_NAME = c.RDB$INDEX_NAME
    WHERE c.RDB$CONSTRAINT_TYPE = 'PRIMARY KEY' AND  c.RDB$RELATION_NAME NOT CONTAINING '$'
    ) on r.RDB$FIELD_NAME = jn AND r.RDB$RELATION_NAME = rn
JOIN RDB$RELATIONS rel on rel.RDB$RELATION_NAME = r.RDB$RELATION_NAME
where F.RDB$FIELD_NAME = R.RDB$FIELD_SOURCE and R.RDB$SYSTEM_FLAG = 0 and R.RDB$RELATION_NAME NOT CONTAINING '$' and rel.RDB$VIEW_SOURCE IS NULL
order by R.RDB$RELATION_NAME, R.RDB$FIELD_POSITION"""
    res = None
    all_data = []
    res = get_fdb(ini, sql, debug=debug)
    if res:
        old_row = None
        fg_first = True
        while res:
            row = res.pop(0)
            if old_row != row[0]:
                #первое вхождение
                if not fg_first:
                    all_data.append(column)
                column = {}
                column['tablename'] = row[0]
                qwe = row[0].upper()
                if column['tablename'] != qwe:
                    column['tablename'] = "\"%s\"" % column['tablename'].strip()
                else:
                    column['tablename'] = column['tablename'].lower()
                column['data'] = []
                c_data = {'name': row[2], 'size': row[3], 'type': row[4], 'notnull': True if row[8]=='1' else False, 'pk': True if row[7]=='1' else False, 'default': row[9] if row[9] else False}
                column['data'].append(c_data)
                fg_first = False
            elif old_row == row[0]:
                c_data = {'name': row[2], 'size': row[3], 'type': row[4], 'notnull': True if row[8]=='1' else False, 'pk': True if row[7]=='1' else False, 'default': row[9] if row[9] else False}
                column['data'].append(c_data)
            old_row = row[0]
        all_data.append(column)
    return all_data

def get_indices_info(ini, debug=True):
    sql = """SELECT r.RDB$INDEX_NAME as I_NAME, r.RDB$RELATION_NAME as T_NAME, s.RDB$FIELD_NAME as F_NAME, s.RDB$FIELD_POSITION as POS_IN_MULT,
    CASE r.RDB$UNIQUE_FLAG
        WHEN 1 THEN 'UNIQUE'
        WHEN 0 THEN ''
    END as uniq, 
    r.RDB$SEGMENT_COUNT SEG,
    CASE r.RDB$INDEX_TYPE 
        WHEN 1 THEN 'DESC'
        WHEN 0 THEN 'ASC'
    END as TYPE
FROM RDB$INDICES r
JOIN RDB$INDEX_SEGMENTS s on r.RDB$INDEX_NAME = s.RDB$INDEX_NAME
WHERE r.RDB$RELATION_NAME NOT CONTAINING '$'
AND r.RDB$INDEX_NAME not in (
    SELECT c.RDB$INDEX_NAME
    FROM RDB$RELATION_CONSTRAINTS c
    WHERE c.RDB$CONSTRAINT_TYPE = 'PRIMARY KEY' and c.RDB$INDEX_NAME NOT CONTAINING '$'
)
AND r.RDB$INDEX_INACTIVE = 0
ORDER by r.RDB$INDEX_NAME, s.RDB$FIELD_POSITION"""
    res = None
    all_data = []
    res = get_fdb(ini, sql, debug=debug)
    if res:
        old_row = None
        fg_first = True
        while res:
            row = res.pop(0)
            if old_row != row[0]:
                #первое вхождение
                if not fg_first:
                    all_data.append(column)
                column = {}
                column['idx_name'] = row[0].strip()
                column['data'] = []
                c_data = {'tab_name': row[1].strip(), 'field_name': row[2].strip(), 'position': row[3], 'unique': row[4].strip(), 'segment': row[5], 'type': row[6].strip()}
                column['data'].append(c_data)
                fg_first = False
            elif old_row == row[0]:
                c_data = {'tab_name': row[1].strip(), 'field_name': row[2].strip(), 'position': row[3], 'unique': row[4].strip(), 'segment': row[5], 'type': row[6].strip()}
                column['data'].append(c_data)
            old_row = row[0]
        all_data.append(column)
    return all_data

def get_triggers_info(ini, debug=True):
    sql = """SELECT r.RDB$TRIGGER_NAME, r.RDB$RELATION_NAME,
    CASE r.RDB$TRIGGER_TYPE
    WHEN 1 THEN 'BEFORE INSERT'
    WHEN 2 THEN 'AFTER INSERT'
    WHEN 3 THEN 'BEFORE UPDATE'
    WHEN 4 THEN 'AFTER UPDATE'
    WHEN 5 THEN 'BEFORE DELETE'
    WHEN 6 THEN 'AFTER DELETE'
    WHEN 17 THEN 'BEFORE INSERT OR UPDATE'
    WHEN 18 THEN 'AFTER INSERT OR UPDATE'
    WHEN 25 THEN 'BEFORE INSERT OR DELETE'
    WHEN 26 THEN 'AFTER INSERT OR DELETE'
    WHEN 27 THEN 'BEFORE UPDATE OR DELETE'
    WHEN 28 THEN 'AFTER UPDATE OR DELETE'
    WHEN 113 THEN 'BEFORE INSERT OR UPDATE OR DELETE'
    WHEN 114 THEN 'AFTER INSERT OR UPDATE OR DELETE'
    WHEN 8192 THEN 'ON CONNECT'
    WHEN 8193 THEN 'ON DISCONNECT'
    WHEN 8194 THEN 'ON TRANSACTION START'
    WHEN 8195 THEN 'ON TRANSACTION COMMIT'
    WHEN 8196 THEN 'ON TRANSACTION ROLLBACK'
    END ACTION,
    r.RDB$TRIGGER_SOURCE
FROM RDB$TRIGGERS r
WHERE r.RDB$SYSTEM_FLAG != 1 AND r.RDB$RELATION_NAME NOT CONTAINING '$'"""
    res = None
    options = []
    res = get_fdb(ini, sql, debug=debug)
    if res:
        for row in res:
            #если в триггере присутствует gen_id, то его пропускаем
            if 'gen_id' in row[3].lower():
                #skip
                continue
            trig = {'name': row[0].strip(), 't_name': row[1].strip(), 'action': row[2].strip(), 'text': row[3].strip()}
            options.append(trig)
    return options

def get_triggers_gen_info(ini, debug=True):
    sql = """SELECT r.RDB$TRIGGER_SOURCE, r.RDB$RELATION_NAME
FROM RDB$TRIGGERS r
WHERE r.RDB$SYSTEM_FLAG != 1 AND r.RDB$RELATION_NAME NOT CONTAINING '$'"""
    res = None
    options = {}
    res = get_fdb(ini, sql, debug=debug)
    if res:
        for rows in res:
            name = rows[1].strip()
            if name != name.upper():
                name = "\"%s\"" % name
            else:
                name = name.lower()
            row = rows[0]
            #если в триггере присутствует gen_id, то это генератор, обрабатываем его
            if 'gen_id' in row.lower():
                row = row.replace('\r', '')
                cols = row.split('\n')
                for col in cols:
                    if 'gen_id' in col and 'new' in col:
                        act = col.split('=')[0].strip()
                        _, field = act.split('.')
                        #name = name.strip()
                        #if name != name.upper():
                            #options["\"%s\"" % name] = field.strip().upper()
                        #else:
                        options[name] = field.strip().upper()
    return options

def get_generators_info(ini, debug=True):
    sql = """SELECT r.RDB$GENERATOR_NAME
FROM RDB$GENERATORS r
WHERE r.RDB$SYSTEM_FLAG != 1
    AND r.RDB$GENERATOR_NAME NOT CONTAINING '$'
ORDER by r.RDB$GENERATOR_NAME"""
    res = None
    res = get_fdb(ini, sql, debug=debug)
    if res:
        r = {}
        for row in res:
            _, ii = row[0].split('GEN_', 1)
            table, field = ii.rsplit('_', 1)
            r[table.strip()] = field.strip()
    return r

def cre_sql_tables(ini, options, seqs, debug=True):
    seqs_names = seqs.keys()
    #print(seqs_names)
    sql_template = """CREATE TABLE IF NOT EXISTS %s ( %s"""
    sql_grant = None
    for opt in options:
        seq = None
        name = opt.get('tablename')
        if name:
            #print(name)
            name = name.strip()
            if name in seqs_names:
                seq = seqs.get(name)
                #print(seq)
            #else:
                #seq = None
            #не нужно, у нас все работают под postgres, а он sysdba
            #sql_grant = "\nGRANT ALL PRIVILEGES ON %s TO postgres;" % name
            fields_insert = []
            data = opt.get('data')
            for row in data:
                r = []
                c_name = row.get('name').strip()
                #преобразуем имена колонок для зарезервированных слов
                c_name = _check_names(ini, c_name)
                r.append(c_name)
                c_type = row.get('type').strip()
                if seq == c_name:
                    c_type = 'SERIAL'
                else:
                    #делаем преобразование типов для pg
                    if c_type == 'blob':
                        c_type = 'bytea'
                c_size = row.get('size')
                #если нужен размер поля для данного типа данных, то устанавливаем
                if c_type in ['varchar', 'char']:
                    c_type += "(%s)"%str(c_size)
                r.append(c_type)
                c_pk = row.get('pk')
                if c_pk:
                    r.append('PRIMARY KEY')
                c_notnull = row.get('notnull')
                if c_notnull:
                    r.append('NOT NULL')
                c_default = row.get('default')
                if c_default != False:
                    c_default = c_default.strip()
                    _, c_default = c_default.split('DEFAULT')
                    c_default = c_default.replace("'", '').strip()
                    r.append("DEFAULT %s" % (c_default if c_default.isdigit() else "'%s'" % c_default))
                r = ' '.join(r)
                fields_insert.append(r)
            fields_insert = ',\n'.join(fields_insert)
            sql = sql_template % (name, fields_insert)
        cou = sql.count('PRIMARY KEY')
        if cou > 1:
            sql = sql.replace('PRIMARY KEY', '')
            sql += get_pkeys(ini, name)
        sql += " );"
        if sql_grant:
            sql += sql_grant
        print('creating table %s' % name, flush=True)
        yield sql

def cre_sql_indices(ini, options, debug=True):
    sql_template = """CREATE %s INDEX IF NOT EXISTS %s on %s (%s %s)"""
    for opt in options:
        name = opt.get('idx_name')
        if name:
            r = []
            data = opt.get('data')
            while data:
                row = data.pop(0)
                i_uniq = row.get('unique')
                r.append(i_uniq)
                r.append(name)
                i_table = row.get('tab_name')
                r.append(i_table)
                i_seg = row.get('segment')
                if i_seg == 1:
                    i_field = row.get('field_name')
                    i_field = _check_names(ini, i_field)
                    r.append(i_field)
                    i_type = row.get('type')
                    r.append(i_type)
                else:
                    ffs = []
                    i_type = row.get('type')
                    while i_seg != 0:
                        i_field = row.get('field_name')
                        i_field = _check_names(ini, i_field)
                        ffs.append(i_field)
                        if len(data) > 0:
                            row = data.pop(0)
                        i_seg -= 1
                    ffs = ','.join(ffs)
                    r.append(ffs)
                    r.append(i_type)
        sql = sql_template % tuple(r)
        print('creating indice %s' % name, flush=True)
        yield sql

def get_pkeys(ini, table, debug=True):
    sql_ins = None
    res = None
    sql = """SELECT fname, fpos, iname
FROM RDB$RELATION_CONSTRAINTS c
join (
    SELECT r.RDB$INDEX_NAME iname,
           s.RDB$FIELD_NAME fname,
           s.RDB$FIELD_POSITION fpos
    FROM RDB$INDICES r
    JOIN RDB$INDEX_SEGMENTS s ON r.RDB$INDEX_NAME = s.RDB$INDEX_NAME
    WHERE r.RDB$RELATION_NAME = '%s'
    ) on c.RDB$INDEX_NAME = iname
ORDER by fpos ASC""" % table.upper()
    res = get_fdb(ini, sql, debug=debug)
    if res:
        sql_ins = """,\nCONSTRAINT %s PRIMARY KEY(%s)"""
        ins = []
        for row in res:
            ins.append("%s" % row[0].strip())
        sql_ins = sql_ins % (res[0][2].strip(), ','.join(ins))
    return sql_ins

def cre_sql_trigers(ini, trig_opt, debug=True):
    sql_template_func = """CREATE OR REPLACE FUNCTION {0}() RETURNS TRIGGER AS $$
{1}
BEGIN
    {2}
RETURN NEW;
END;
$$ LANGUAGE plpgsql;""" # 0 - имя функции, 1 - блок DECLARE с внутренними переменными, 2 - текст функции
    sql_template = """DROP TRIGGER IF EXISTS {0} ON {1};
CREATE TRIGGER {0}
{2} ON {1} FOR EACH ROW EXECUTE PROCEDURE {3}();""" # 0 - имя триггера, 1 - имя таблицы, 2 - тип действия, например (BEFORE INSERT), 3 - имя функции
    for trig in trig_opt:
        trig_name = trig.get('name')
        func_name = trig_name + '_FUNC'
        table_name = trig.get('t_name')
        trig_action = trig.get('action')
        trig_text = trig.get('text')
        trig_text.replace('\r', '')
        trig_text_list = trig_text.split('\n')
        new_trig_text_list = []
        fg_dc = False
        declare = []
        into_vars = []
        for row in trig_text_list:
            row = row.strip()
            row = row.upper()
            if 'as' in row.lower() or 'begin' in row.lower() or 'end' in row.lower() or row.strip().startswith('--') or not row:
                continue
            if 'DECLARE' in row:
                #есть объявления переменных
                #убираем из text объявления
                #сделаем добавление в секцию DECLARE
                new_row = row.replace('DECLARE', '')
                new_row = new_row.replace('VARIABLE', '')
                declare.append(new_row)
                continue
            if 'INTO' in row and 'INSERT' not in row:
                _, i_v = row.strip().split(':')
                into_vars.append(i_v.replace(';', ''))
                row = row.replace('INTO:', '').replace('INTO', '').replace(':%s'%i_v, ';').replace(i_v, ';')
            row = row.replace('CURRENT_TIMESTAMP', 'now()')
            new_trig_text_list.append(row)
        trig_text = '\n'.join(new_trig_text_list)
        if len(into_vars) > 0:
            new_stri = []
            trig_text = trig_text.split('SELECT')
            for i, a in enumerate(trig_text):
                if len(declare) > 0:
                    a = a.replace(':', '')
                if i < len(into_vars):
                    new_stri.append(a + into_vars[i] + ':=')
                else:
                    new_stri.append(a)
            trig_text = 'SELECT'.join(new_stri)
        if len(declare) > 0:
            declare = 'DECLARE ' + '\n'.join(declare)
        else:
            declare = ''
        if 'if' in trig_text.lower():
            if_pos = trig_text.lower().find('if')
            then_pos = trig_text.lower().find('then', if_pos)
            if 'else' not in trig_text.lower():
                semi_pos = trig_text.lower().find(';', then_pos)
            else:
                else_pos = trig_text.lower().find('else', then_pos)
                semi_pos = trig_text.lower().find(';', else_pos)
            trig_text = trig_text[:semi_pos+1] + '\nEND IF;\n' + trig_text[semi_pos+1:]
        ddot_inx = trig_text.count(':=')
        ddot = 0
        for i in range(ddot_inx):
            ddot = trig_text.find(':=', ddot)
            semi_pos = trig_text.find(';', ddot)
            trig_text = trig_text[:ddot+2] + '(' + trig_text[ddot+2:semi_pos] + ')' + trig_text[semi_pos:]
            ddot = semi_pos
        sql_func = sql_template_func.format(func_name, declare, trig_text)
        if 'BEFORE DELETE' in trig_action:
            sql_func = sql_func.replace('RETURN NEW', 'RETURN OLD')
        sql_tri = sql_template.format(trig_name, table_name, trig_action, func_name)
        sql = '\n'.join([sql_func, sql_tri])
        print('creating trigger %s' % trig_name)
        yield sql

def set_exec(ini, sql, debug=False, fetch=False):
    result = None
    with PGConn(ini, debug=debug) as db:
        db.execute(sql)
        if fetch:
            result = db.fetch()
    if result:
        return result
    else:
        return

def get_pg_tables(ini, debug=False):
    res = None
    name = None
    fields = None
    old_name = None
    sql = """select table_name, column_name, ordinal_position from information_schema.columns where table_schema='public' order by table_name, ordinal_position;"""
    res = set_exec(ini, sql, debug=debug, fetch=1)
    if res:
        for row in res:
            name = row[0]
            if name != name.lower():
                name = "\"%s\"" % name
            else:
                name = name.lower()
            if old_name == None:
                #первая строка
                fields = []
                fields.append(row[1])
                old_name = name
                continue
            if name == old_name:
                fields.append(row[1])
            else:
                yield old_name, fields
                fields = []
                fields.append(row[1])
            old_name = name
        yield old_name, fields

def _gen_get_data(kwargs):
    ini = kwargs.get('ini')
    debug = kwargs.get('debug')
    fields = kwargs.get('fields')
    name = kwargs.get('name')
    c1 = kwargs.get('c1')
    c2 = kwargs.get('c2')
    sql_fields = kwargs.get('sql_fields')
    cpu = kwargs.get('cpu')
    pk = kwargs.get('pk')
    print('start pump: thread-> %s ; from position-> %s' %(cpu, c1), flush=True)
    sql_template = """select {0} from {1} r order by r.{2} asc rows {3} to {4}"""
    sql_t = """insert into {0} ({1}) values ({2}) ON CONFLICT DO NOTHING;"""
    sql = sql_template.format(sql_fields, name, pk, c1, c2)
    #print(sql)
    params = []
    rows = None
    ccc = 0
    #print("sql\t", sql)
    res = get_fdb(ini, sql, debug=debug)
    if res:
        for row in res:
            row_ins = []
            for col in row:
                row_ins.append(col)
            params.append(row_ins)
            ccc += 1
    #проверяем, может какое поле bytea
    byteas = []
    sql_check = """select table_name, column_name, data_type
from information_schema.columns
where table_name = '%s' and data_type = 'bytea';""" % name
    res = None
    #print(sql_check)
    res = set_exec(ini, sql_check, debug=debug, fetch=True)
    #print("_"*20)
    #print(res)
    if res:
        for row in res:
            byteas.append(row[1].lower())
    qqs = []
    iis = []
    for i, f in enumerate(fields):
        if f in byteas:
            iis.append(i)
        q = '%s'
        qqs.append(q)
    values = ','.join(qqs)
    for i in iis:
        for para in params:
            if para[i]:
                #print(para[i])
                try:
                    data = para[i].encode()
                except:
                    #print('eeeeeeeee', para[i], sep="\t")
                    data = para[i]
                para[i] = psycopg2.Binary(data)
                #print(para[i])
    sql_fields = ','.join([_check_names(ini, fi) for fi in fields])
    with PGConn(ini, debug=debug) as db:
        sql = sql_t.format(name, sql_fields, values)
        #print(sql)
        #print(params)
        db.executemany(sql, params)
    #print('ccc->', ccc)
    return ccc

def _check_names(ini, in_field):
    if in_field in ini.params.excludes:
        in_field = '"%s"' % in_field
    return in_field

def get_fb_data(ini, name, fields, only=None, exclude=None, debug=False):
    print('pumping %s' % name, flush=True)
    #print(only)
    if only and name.lower() != only.lower():
        print('skipping', flush=True)
        return
    if exclude and name.lower() in exclude:
        print('skipping', flush=True)
        return
    res = None
    pg_name = name
    #print("*"*20)
    #print(name)
    #print(name.upper())
    if name != name.lower():
        fb_name = "\"%s\"" % name
    else:
        fb_name = name.upper()
    sql = """select count(*) from %s""" % fb_name
    #print(sql)
    res = get_fdb(ini, sql, debug=debug)
    if res:
        total_count = int(res[0][0])
    else:
        return
    if total_count == 0:
        return
    c1 = 1
    cnt = ini.params.potion
    c2 = c1 + cnt - 1
    sql = """select R.RDB$RELATION_NAME, R.RDB$FIELD_NAME, tt PK
from RDB$FIELDS F, RDB$RELATION_FIELDS R
left JOIN (
    SELECT c.RDB$RELATION_NAME rn, seg.RDB$FIELD_NAME jn, c.RDB$CONSTRAINT_TYPE tt
    FROM RDB$RELATION_CONSTRAINTS c
    JOIN RDB$INDEX_SEGMENTS seg on seg.RDB$INDEX_NAME = c.RDB$INDEX_NAME
    WHERE c.RDB$CONSTRAINT_TYPE = 'PRIMARY KEY' AND  c.RDB$RELATION_NAME NOT CONTAINING '$'
    ) on r.RDB$FIELD_NAME = jn AND r.RDB$RELATION_NAME = rn
JOIN RDB$RELATIONS rel on rel.RDB$RELATION_NAME = r.RDB$RELATION_NAME
where F.RDB$FIELD_NAME = R.RDB$FIELD_SOURCE and R.RDB$SYSTEM_FLAG = 0 and R.RDB$RELATION_NAME NOT CONTAINING '$' and rel.RDB$VIEW_SOURCE IS NULL
AND r.RDB$RELATION_NAME = '%s'
AND tt is NOT NULL
order by R.RDB$RELATION_NAME, R.RDB$FIELD_POSITION""" % fb_name
    sql_ind = """SELECT ic.oid,pg_get_indexdef(ic.oid),ic.relname AS name, am.amname, i.indisprimary AS pri,
i.indisunique AS uni, i.indkey AS fields, i.indclass AS fopclass,
i.indisclustered, ic.oid AS indid, c.oid AS relid, ds.description,
u.usename, pg_get_expr(i.indexprs, i.indrelid) AS expr,
ts.spcname, pg_get_expr(i.indpred, i.indrelid) AS wh,
cn.oid IS NOT NULL AS iscn, cn.oid as constroid
FROM pg_index i INNER JOIN pg_class c ON i.indrelid = c.oid
INNER JOIN pg_class ic ON i.indexrelid = ic.oid
INNER JOIN pg_am am ON ic.relam = am.oid
LEFT OUTER JOIN pg_description ds ON ds.objoid = ic.oid
LEFT OUTER JOIN pg_user u ON u.usesysid = ic.relowner
LEFT OUTER JOIN pg_constraint cn ON i.indrelid = cn.conrelid AND ic.relname = cn.conname
LEFT OUTER JOIN pg_tablespace ts ON ts.oid = ic.reltablespace
WHERE
c.oid = ('%s')::regclass::oid and not i.indisprimary
ORDER BY ic.relname;"""% fb_name
    res = None
    res = get_fdb(ini, sql, debug=debug)
    print(res)
    if res:
        pk = res[0][1].strip()
    else:
        pk = 'RDB$DB_KEY'
    f_ins = []
    for field in fields:
        field = _check_names(ini, field)
        f_ins.append('r.' + field)
    sql_fields = ','.join(f_ins)
    cc = 0
    indices_drop = None
    indeces_sqls = None
    #отключаем индексы (удаляем)
    res = set_exec(ini, sql_ind, debug=debug, fetch=1)
    if res:
        indices_drop, indeces_sqls = _get_pg_indeces(ini, res)
        if indices_drop:
            set_exec(ini, indices_drop, debug=debug)
            #db.execute(indices_drop)
    cpu_number = ini.params.cpu
    while total_count > 0:
        #создаем пулл в зависимости от количества ядер
        pool = ThreadPool(cpu_number)
        p_list = []
        for cpu in range(cpu_number):
            if total_count < 0:
                pump_params = {'ini': ini, 'sql_fields': sql_fields, 'name': name, 'c1': 1, 'c2': 0, 'pk': pk, 'fields': fields, 'debug':debug, 'cpu': cpu}
            else:
                pump_params = {'ini': ini, 'sql_fields': sql_fields, 'name': name, 'c1': c1, 'c2': c2, 'pk': pk, 'fields': fields, 'debug':debug, 'cpu': cpu}
            p_list.append(pump_params)
            c2 = c2 + cnt
            c1 = c1 + cnt
            total_count -= cnt
        results = pool.map(_gen_get_data, p_list)
        pool.close()
        pool.join()
        q = 0
        for r in results:
            q += int(r)
        cc += q
    #включаем индексы (создаем)
    if indeces_sqls:
        set_exec(ini, indeces_sqls, debug=debug)
    print('total->', cc, flush=True)


def _get_pg_indeces(ini, res):
    indices = []
    drops = []
    sql_drop = "DROP INDEX IF EXISTS %s ;"
    for row in res:
        table = row[1].split('ON')[1].strip()
        table = table.split()[0].strip()
        indices.append(row[1]+';')
        drops.append(sql_drop % row[2])
    return '\n'.join(drops), '\n'.join(indices)

def pg_check(ini, sql, debug=False):
    res = None
    #res = set_exec(ini, sql, debug=debug, fetch=1)
    with PGConn(ini, debug=debug) as db:
        db.request(sql)
        res = db.fetch()
    return res

def get_generator_value(ini, debug=False):
    sql = """SELECT r.RDB$TRIGGER_SOURCE, r.RDB$RELATION_NAME
FROM RDB$TRIGGERS r
WHERE r.RDB$SYSTEM_FLAG != 1 AND r.RDB$RELATION_NAME NOT CONTAINING '$'"""
    sql_temp = """select DISTINCT %s from RDB$GENERATORS"""
    sql_template = """select setval('%s', %s, true);"""
    res = None
    options = {}
    res = get_fdb(ini, sql, debug=debug)
    if res:
        for rows in res:
            name = rows[1].strip()
            if name != name.upper():
                name = "\"%s\"" % name
            else:
                name = name.lower()
            row = rows[0]
            #если в триггере присутствует gen_id, то это генератор, обрабатываем его
            if 'gen_id' in row:
                row = row.replace('\r', '')
                cols = row.split('\n')
                act = None
                for col in cols:
                    if 'gen_id' in col and 'new' in col:
                        act = col.split('=')[1].strip()
                        act = act.replace(';', '').replace('1)', '0)')
                        a1 = col.split('=')[0].strip()
                        _, field = a1.split('.')
                        break
                res = None
                if act:
                    sql = sql_temp % act
                    res = get_fdb(ini, sql, debug=debug)
                    if res:
                        sequence = '_'.join([name, field, 'seq'])
                        if sequence.find('"') != -1:
                            sequence = "\"%s\"" % sequence.replace('"', '') 
                        sql = sql_template % (sequence, int(res[0][0]))
                        print('set generator for %s' % name)
                        yield sql

def get_fdb(ini, sql, debug=False):
    result = None
    with FBConn(ini) as fdb:
        fdb.request(sql)
        result = fdb.fetch()
    return result

