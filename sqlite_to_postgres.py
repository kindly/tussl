import subprocess


def sqlite_to_postgres(context, data_dict):

    connection = context['connection']
    sqlite_connection = context['sqlite_connection']
    table_name = data_dict['table_name']
    target_table_name = data_dict.get('target_table_name', data_dict['table_name'])

    assert '"' not in table_name
    assert '"' not in target_table_name

    results = sqlite_connection.execute('''pragma table_info("%s")''' % table_name)

    type_conv =dict(
        INT='bigint',
        INTEGER='bigint',
        TINYINT='bigint',
        SMALLINT='bigint',
        MEDIUMINT='bigint',
        BIGINT='bigint',
        UNSIGNED='bigint',
        INT2='bigint',
        INT8='bigint',
        CHARACTER='text',
        VARCHAR='text',
        VARYING='text',
        NCHAR='text',
        NATIVE='text',
        NVARCHAR='text',
        TEXT='text',
        CLOB='text',
        BLOB='bytea',
        REAL='double precision',
        DOUBLE='double precision',
        FLOAT='double precision',
        NUMERIC='double precision',
        DECIMAL='double precision',
        BOOLEAN='int',
        DATE='text',
        DATETIME='text',
    )

    table_def = results.fetchall()

    defs = []
    for row in table_def:
        posgres_type = None
        for sqlite_type in type_conv:
            if sqlite_type.lower() in row[2].lower():
                posgres_type = type_conv[sqlite_type]
                break
        assert posgres_type
        assert '"' not in row[1]
        defs.append('"%s" %s' %  (row[1], posgres_type))


    sql = 'create table if not exists %s(%s)' % (target_table_name, ', '.join(defs))

    connection.execute(sql)

    try:
        proc = subprocess.Popen(['sqlite3', '-csv', sqlite_connection.engine.url.database, 'select * from "%s"' % table_name], stdout=subprocess.PIPE)
        con = connection.connect()
        cursor = con.connection.cursor()
        cursor.copy_expert('copy "%s" from STDOUT with CSV;' % target_table_name, proc.stdout, size=30000)
        cursor.execute("commit;")
    finally:
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
    connection = sqlalchemy.create_engine('postgres://david:ytrewq@localhost/tussl_test', echo=True).connect()
    sqlite_connection = sqlalchemy.create_engine('sqlite:////home/david/projects/test.sqlite').connect()

    context = {'connection': connection,
               'sqlite_connection': sqlite_connection}
    data_dict = {'table_name': 'places100000',
                 'target_table_name': 'places2'}

    sqlite_to_postgres(context, data_dict)
    print init_time - time.time()









