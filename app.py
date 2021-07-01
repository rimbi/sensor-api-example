from flask import Flask, render_template, request, Response, abort
from flask.json import jsonify
import json
import sqlite3
import time

app = Flask(__name__)

# Setup the SQLite DB
conn = sqlite3.connect('database.db')
conn.execute('CREATE TABLE IF NOT EXISTS readings (device_uuid TEXT, type TEXT, value INTEGER, date_created INTEGER)')
conn.close()

def get_conn_and_cursor():
    """
    Returns the db that we want and open the connection
    """
    if app.config['TESTING']:
        conn = sqlite3.connect('test_database.db')
    else:
        conn = sqlite3.connect('database.db')
    conn.row_factory = sqlite3.Row
    return (conn, conn.cursor())

def query_with_time_range(request, query):
    """
    Returns the updated query if a time range is provided
    """
    start = request.args.get('start')
    end = request.args.get('end')
    if start and end:
        query += f' AND date_created BETWEEN {start} and {end}'
    return query

def query_with_type(request, query, optional = False):
    """
    Returns the updated query if a time range is provided
    """
    type = request.args.get('type')
    if type is None:
        if optional:
            return query
        abort (Response('Missing type parameter', 400))
    if type.lower() not in ['humidity', 'temperature']:
        abort (Response(f'Invalid type valaue: {type}', 400))
    query += f' AND type="{type}"'
    return query

def get_values(request, query, sort=False, post_process_rows=None, post_process_query=None):
    """
    Returns the value in accordance with the query parameters
    """
    query = query_with_type(request, query)
    query = query_with_time_range(request, query)

    if sort:
        query += ' ORDER BY value'

    if post_process_query:
        query = post_process_query(query)
    print(query)
    # Execute the query
    conn, cur = get_conn_and_cursor()
    cur.execute(query)
    rows = cur.fetchall()

    if post_process_rows:
        rows = post_process_rows(rows)

    return jsonify(dict(zip(['value'], rows[0]))), 200

@app.route('/devices/<string:device_uuid>/readings/', methods = ['POST', 'GET'])
def request_device_readings(device_uuid):
    """
    This endpoint allows clients to POST or GET data specific sensor types.

    POST Parameters:
    * type -> The type of sensor (temperature or humidity)
    * value -> The integer value of the sensor reading
    * date_created -> The epoch date of the sensor reading.
        If none provided, we set to now.

    Optional Query Parameters:
    * start -> The epoch start time for a sensor being created
    * end -> The epoch end time for a sensor being created
    * type -> The type of sensor value a client is looking for
    """

    # Set the db that we want and open the connection
    conn, cur = get_conn_and_cursor()
   
    if request.method == 'POST':
        # Grab the post parameters
        post_data = json.loads(request.data)
        sensor_type = post_data.get('type')
        value = post_data.get('value')
        date_created = post_data.get('date_created', int(time.time()))

        # Insert data into db
        cur.execute('insert into readings (device_uuid,type,value,date_created) VALUES (?,?,?,?)',
                    (device_uuid, sensor_type, value, date_created))
        
        conn.commit()

        # Return success
        return 'success', 201
    else:
        # Execute the query
        query = 'select * from readings where device_uuid="{}"'.format(device_uuid)
        query = query_with_type(request, query, optional=True)
        query = query_with_time_range(request, query)
        cur.execute(query)
        rows = cur.fetchall()

        # Return the JSON
        return jsonify([dict(zip(['device_uuid', 'type', 'value', 'date_created'], row)) for row in rows]), 200

@app.route('/devices/<string:device_uuid>/readings/max/', methods = ['GET'])
def request_device_readings_max(device_uuid):
    """
    This endpoint allows clients to GET the max sensor reading for a device.

    Mandatory Query Parameters:
    * type -> The type of sensor value a client is looking for

    Optional Query Parameters
    * start -> The epoch start time for a sensor being created
    * end -> The epoch end time for a sensor being created
    """
    query = f'select MAX(value) from readings where device_uuid="{device_uuid}"'
    return get_values(request, query)

@app.route('/devices/<string:device_uuid>/readings/min/', methods = ['GET'])
def request_device_readings_min(device_uuid):
    """
    This endpoint allows clients to GET the min sensor reading for a device.

    Mandatory Query Parameters:
    * type -> The type of sensor value a client is looking for

    Optional Query Parameters
    * start -> The epoch start time for a sensor being created
    * end -> The epoch end time for a sensor being created
    """
    query = f'select MIN(value) from readings where device_uuid="{device_uuid}"'
    return get_values(request, query)

@app.route('/devices/<string:device_uuid>/readings/median/', methods = ['GET'])
def request_device_readings_median(device_uuid):
    """
    This endpoint allows clients to GET the median sensor reading for a device.

    Mandatory Query Parameters:
    * type -> The type of sensor value a client is looking for

    Optional Query Parameters
    * start -> The epoch start time for a sensor being created
    * end -> The epoch end time for a sensor being created
    """

    def pick_median(rows):
        """
        Given values, returns the median value
        """
        if not rows:
            return [[]]
        l = len(rows)
        if l % 2 == 0:
            return [[ (rows[l//2 -1][0] + rows[l//2][0])/2 ]]
        return [[ rows[l//2][0] ]]

    query = f'select value from readings where device_uuid="{device_uuid}"'
    return get_values(request, query, sort=True, post_process_rows=pick_median)

@app.route('/devices/<string:device_uuid>/readings/mean/', methods = ['GET'])
def request_device_readings_mean(device_uuid):
    """
    This endpoint allows clients to GET the mean sensor readings for a device.

    Mandatory Query Parameters:
    * type -> The type of sensor value a client is looking for

    Optional Query Parameters
    * start -> The epoch start time for a sensor being created
    * end -> The epoch end time for a sensor being created
    """

    query = f'select AVG(value) from readings where device_uuid="{device_uuid}"'
    return get_values(request, query)

@app.route('/devices/<string:device_uuid>/readings/mode/', methods = ['GET'])
def request_device_readings_mode(device_uuid):
    """
    This endpoint allows clients to GET the mode sensor readings for a device.

    Mandatory Query Parameters:
    * type -> The type of sensor value a client is looking for

    Optional Query Parameters
    * start -> The epoch start time for a sensor being created
    * end -> The epoch end time for a sensor being created
    """

    def process_query(query):
        return query + ' LIMIT 1'

    query = f'select value from readings where device_uuid="{device_uuid}"'
    return get_values(request, query, post_process_query=process_query)

@app.route('/devices/<string:device_uuid>/readings/quartiles/', methods = ['GET'])
def request_device_readings_quartiles(device_uuid):
    """
    This endpoint allows clients to GET the 1st and 3rd quartile
    sensor reading value for a device.

    Mandatory Query Parameters:
    * type -> The type of sensor value a client is looking for
    * start -> The epoch start time for a sensor being created
    * end -> The epoch end time for a sensor being created
    """

    return 'Endpoint is not implemented', 501

@app.route('/devices/summary', methods = ['GET'])
def request_readings_summary():
    """
    This endpoint allows clients to GET a full summary
    of all sensor data in the database per device.

    Optional Query Parameters
    * type -> The type of sensor value a client is looking for
    * start -> The epoch start time for a sensor being created
    * end -> The epoch end time for a sensor being created
    """

    return 'Endpoint is not implemented', 501

if __name__ == '__main__':
    app.run()
