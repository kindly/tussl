
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

def convert_jsonable(value):
    if any([isinstance(value, basestring),
           isinstance(value, int),
           isinstance(value, float),
           isinstance(value, bool)]):
        return value
    if value is None:
        return None
    return str(value)

def dictize_results(context, results):
    jsonable = context.get('jsonable', False)
    result = {}
    data = []
    fields = []
    result['fields'] = fields
    result['data'] = data

    for field in results.cursor.description:
        fields.append(
            {'name': field.name,
             'type': get_type(context, field.type_code)}
        )

    for row in results:
        row_data = {}
        for num, value in enumerate(row):
            if jsonable:
                value = convert_jsonable(value)
            row_data[fields[num]['name']] = value
        result['data'].append(row_data)

    result['count'] = results.rowcount

    return result

def execute_sql(context, sql, **kw):
    connection = context['connection']
    results = connection.execute(sql, **kw)

    #not a select statement
    if not results.cursor:
        if results.rowcount > 0:
            return {'count': results.rowcount}
        else:
            #if create/drop table
            return {}

    return dictize_results(context, results)
