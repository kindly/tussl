import sqlalchemy
import execute
import datetime
import pprint

def test_basic_execute():

    engine = sqlalchemy.create_engine('postgres://david:ytrewq@localhost/tussl_test')
    engine.execute('drop table if exists basic')
    engine.execute('create table basic ( a int, b text, c text[], d timestamp)')
    engine.execute('''insert into basic values (2, 'text', '{text1, text2}', '2005-01-01') ''')

    connection = engine.connect()
    context = {'connection': connection}
    results = execute.execute_sql(context, 'select * from basic')
    
    assert results == {
        'count': 1,
        'data': [{'a': 2, 'b': u'text', 'c': ["text1", "text2"], 'd': datetime.datetime(2005, 1, 1, 0, 0)}],
        'fields': [
            {'name': 'a', 'type': u'int4'},
            {'name': 'b', 'type': u'text'},
            {'name': 'c', 'type': u'_text'},
            {'name': 'd', 'type': u'timestamp'}
        ]
    }, pprint.pprint(results)



    results = execute.execute_sql(context, '''insert into basic values (2, 'text', '{text1, text2}', '2005-01-01') ''')
    assert results == {'count': 1}, results

    results = execute.execute_sql({'connection': connection}, '''drop table if exists basic2''')
    assert results == {}, results

    results = execute.execute_sql({'connection': connection}, '''create table basic2 ( a int, b text, c text[], d timestamp)''')
    assert results == {}, results

    results = execute.execute_sql(context, 'insert into basic2 select * from basic')
    assert results == {'count': 2}, results

    results = execute.execute_sql(context, 'select * from basic2')

    assert results == {
        'count': 2,
        'data': [{'a': 2,
                   'b': u'text',
                   'c': ['text1', 'text2'],
                   'd': datetime.datetime(2005, 1, 1, 0, 0)},
                  {'a': 2,
                   'b': u'text',
                   'c': ['text1', 'text2'],
                   'd': datetime.datetime(2005, 1, 1, 0, 0)}],
         'fields': [{'name': 'a', 'type': u'int4'},
                    {'name': 'b', 'type': u'text'},
                    {'name': 'c', 'type': u'_text'},
                    {'name': 'd', 'type': u'timestamp'}]}, pprint.pprint(results)
