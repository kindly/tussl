import flask
import execute
import sqlalchemy
from datetime import timedelta
from functools import update_wrapper

app = flask.Flask(__name__)

def crossdomain(f):
    def decorator(*args, **kwargs):
        if flask.request.method == 'OPTIONS':
            resp = flask.current_app.make_default_options_response()
        else:
            resp = flask.make_response(f(*args, **kwargs))

        h = resp.headers
        h['Access-Control-Allow-Origin'] = "*"
        h['Access-Control-Allow-Methods'] = "POST, PUT, GET, DELETE"
        h['Access-Control-Allow-Headers'] = "Content-Type"
        return resp
    decorator.provide_automatic_options = False
    return decorator

@app.route("/<database>", methods=['post','options'])
@crossdomain
def sql(database):
    engine = sqlalchemy.create_engine('postgres://david:ytrewq@localhost/%s' % database)
    connection = engine.connect()

    try:
        context = {'connection': connection, 'jsonable': True}
        sql = flask.request.json['sql']
        result = execute.execute_sql(context, sql)
    finally:
        connection.close()

    resp = flask.jsonify(**result)
    return resp


if __name__ == "__main__":
    app.run(debug=True)
