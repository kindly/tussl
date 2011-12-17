import subprocess

_pg_types = {}

def get_type(context, oid):
    if not _pg_types:
        connection = context['connection']
        results = connection.execute(
            'select oid, typname from pg_type;'
        )
        for result in results:
            _pg_types[result[0]] = result[1]

    return _pg_types[oid]

type_conv =dict(
    bool='boolean',
    bytea='blob',
    int8='int',
    int2='int',
    int4='int',
    float4='float',
    float8='float',
    date='date',
    time='time',
    timestamp='datetime',
    timestamptz='datetime',
    numeric='numeric'
)

def postgres_to_sqlite(context, data_dict):

    connection = context['connection']
    sqlite_connection = context['sqlite_connection']
    table_name = data_dict['table_name']

    #limit size
    #SELECT pg_size_pretty(pg_relation_size('big_table'));

    target_table_name = data_dict.get('target_table_name', data_dict['table_name'])

    assert '"' not in table_name
    assert '"' not in target_table_name

    results = connection.execute('''select * from "%s" limit 1''' % table_name)

    fields = []
    for field in results.cursor.description:
        fields.append(
            {'name': field.name,
             'type': get_type({'connection': connection}, field.type_code)}
        )

    defs = []
    for field in fields:
        name, type = field['name'], field['type']
        assert '"' not in name
        defs.append('"%s" %s' %  (name, type_conv.get(type, 'text')))

    sql = 'create table if not exists "%s"(%s)' % (target_table_name, ', '.join(defs))
    sqlite_connection.execute(sql)


    select_fields = []
    for field in fields:
        name, type = field['name'], field['type']
        select_fields.append('''btrim(quote_nullable("%s"),'E')''' % name)

    sql_to_run = '''select 'insert into "%s" values(' || concat_ws(', ', %s) || ') ;' from "%s"''' % (
        target_table_name, ', '.join(select_fields), table_name
    )

    try:
        proc = subprocess.Popen(['sqlite3', sqlite_connection.engine.url.database], stdin=subprocess.PIPE)
        #class a: pass
        #proc = a()
        #proc.stdin = open('moo' , 'w+')
        cursor = connection.connection.cursor()
        proc.stdin.write('PRAGMA synchronous = OFF; PRAGMA journal_mode = MEMORY; begin;')
        cursor.copy_expert("copy (%s) to STDIN;" % sql_to_run, proc.stdin, size=30000)
        proc.stdin.write('commit;')
        proc.stdin.close()

    finally:
        #return
        for i in range(1,100):
            if proc.poll() is not None:
                break
            time.sleep(0.1)
        else:
            proc.terminate()


if __name__ == '__main__':

    import sqlalchemy
    import time

    init_time = time.time()
    #connection = sqlalchemy.create_engine('postgres://david:ytrewq@localhost/tussl_test', echo=True).connect()
    connection = sqlalchemy.create_engine('postgres://david:ytrewq@localhost/dgu', echo=True).connect()
    sqlite_connection = sqlalchemy.create_engine('sqlite:////home/david/projects/test.sqlite').connect()

    context = {'connection': connection,
               'sqlite_connection': sqlite_connection}
    data_dict = {'table_name': 'revision'}

    postgres_to_sqlite(context, data_dict)
    print init_time - time.time()




