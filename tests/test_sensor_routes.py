import json
import pytest
import sqlite3
import time
import unittest

from app import app

class SensorRoutesTestCases(unittest.TestCase):

    def setUp(self):
        # Setup the SQLite DB
        conn = sqlite3.connect('test_database.db')
        conn.execute('DROP TABLE IF EXISTS readings')
        conn.execute('CREATE TABLE IF NOT EXISTS readings (device_uuid TEXT, type TEXT, value INTEGER, date_created INTEGER)')
        
        self.device_uuid = 'test_device'

        # Setup some sensor data
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        
        cur.execute('insert into readings (device_uuid,type,value,date_created) VALUES (?,?,?,?)',
                    (self.device_uuid, 'temperature', 22, int(time.time()) - 100))
        cur.execute('insert into readings (device_uuid,type,value,date_created) VALUES (?,?,?,?)',
                    (self.device_uuid, 'temperature', 50, int(time.time()) - 50))
        cur.execute('insert into readings (device_uuid,type,value,date_created) VALUES (?,?,?,?)',
                    (self.device_uuid, 'temperature', 100, int(time.time())))

        cur.execute('insert into readings (device_uuid,type,value,date_created) VALUES (?,?,?,?)',
                    ('other_uuid', 'temperature', 22, int(time.time())))
        conn.commit()

        app.config['TESTING'] = True

        self.client = app.test_client

    def test_device_readings_get(self):
        # Given a device UUID
        # When we make a request with the given UUID
        request = self.client().get('/devices/{}/readings/'.format(self.device_uuid))

        # Then we should receive a 200
        self.assertEqual(request.status_code, 200)

        # And the response data should have three sensor readings
        self.assertTrue(len(json.loads(request.data)) == 3)

    def test_device_readings_post(self):
        # Given a device UUID
        # When we make a request with the given UUID to create a reading
        request = self.client().post('/devices/{}/readings/'.format(self.device_uuid), data=
            json.dumps({
                'type': 'temperature',
                'value': 100 
            }))

        # Then we should receive a 201
        self.assertEqual(request.status_code, 201)

        # And when we check for readings in the db
        conn = sqlite3.connect('test_database.db')
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute('select * from readings where device_uuid="{}"'.format(self.device_uuid))
        rows = cur.fetchall()

        # We should have three
        self.assertTrue(len(rows) == 4)

    def test_device_readings_get_temperature(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's temperature data only.
        """
        # Given a device UUID
        # When we make a request with the given UUID
        request = self.client().get(
            '/devices/{}/readings/'.format(self.device_uuid),
            query_string={'type': 'temperature'}
        )

        # Then we should receive a 200
        self.assertEqual(request.status_code, 200)

        self.assertEqual(len(json.loads(request.data)), 3)

    def test_device_readings_get_humidity(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's humidity data only.
        """
        # Given a device UUID
        # When we make a request with the given UUID
        request = self.client().get(
            '/devices/{}/readings/'.format(self.device_uuid),
            query_string={'type': 'humidity'}
        )

        # Then we should receive a 200
        self.assertEqual(request.status_code, 200)

        self.assertEqual(len(json.loads(request.data)), 0)

    def test_device_readings_get_past_dates(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's sensor data over
        a specific date range. We should only get the readings
        that were created in this time range.
        """
        request = self.client().get(
            '/devices/{}/readings/'.format(self.device_uuid),
            query_string={'start': 'temperature'}
        )

        # Then we should receive a 200
        self.assertEqual(request.status_code, 200)

        self.assertEqual(len(json.loads(request.data)), 3)

    def test_device_readings_min(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's min sensor reading.
        """
        request = self.client().get(
            '/devices/{}/readings/min/'.format(self.device_uuid),
            query_string={'type': 'temperature'}
        )

        # Then we should receive a 200
        self.assertEqual(request.status_code, 200)

        self.assertEqual(json.loads(request.data)['value'], 22)

    def test_device_readings_max(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's max sensor reading.
        """
        request = self.client().get(
            '/devices/{}/readings/max/'.format(self.device_uuid),
            query_string={'type': 'temperature'}
        )

        # Then we should receive a 200
        self.assertEqual(request.status_code, 200)

        self.assertEqual(json.loads(request.data)['value'], 100)

    def test_device_readings_median(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's median sensor reading.
        """
        request = self.client().get(
            '/devices/{}/readings/median/'.format(self.device_uuid),
            query_string={'type': 'temperature'}
        )

        # Then we should receive a 200
        self.assertEqual(request.status_code, 200)
        import math
        value = json.loads(request.data)['value']
        self.assertEqual(value, 50)

    def test_device_readings_mean(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's mean sensor reading value.
        """
        request = self.client().get(
            '/devices/{}/readings/mean/'.format(self.device_uuid),
            query_string={'type': 'temperature'}
        )

        # Then we should receive a 200
        self.assertEqual(request.status_code, 200)
        import math
        min = json.loads(request.data)['value']
        self.assertTrue(math.isclose(min, 57.33, abs_tol=0.01))

    def test_device_readings_mode(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's mode sensor reading value.
        """
        request = self.client().get(
            '/devices/{}/readings/mode/'.format(self.device_uuid),
            query_string={'type': 'temperature'}
        )

        # Then we should receive a 200
        self.assertEqual(request.status_code, 200)
        self.assertEqual(len(json.loads(request.data)), 1)

    def test_invalid_type(self):
        """
        This test should be implemented. The goal is to test that
        we are not able to query for anything other than 'temperature' 
        and 'humidity'.
        """
        request = self.client().get(
            '/devices/{}/readings/mean/'.format(self.device_uuid),
            query_string={'type': 'distance'}
        )

        # Then we should receive a 200
        self.assertEqual(request.status_code, 400)

    def test_invalid_value_range(self):
        """
        This test should be implemented. The goal is to test that
        we are not able to query for anything other than 'temperature' 
        and 'humidity'.
        """
        request = self.client().post('/devices/{}/readings/'.format(self.device_uuid), data=
            json.dumps({
                'type': 'temperature',
                'value': 101 
            }))

        # Then we should receive a 201
        self.assertEqual(request.status_code, 400)

    # def test_device_readings_quartiles(self):
    #     """
    #     This test should be implemented. The goal is to test that
    #     we are able to query for a device's 1st and 3rd quartile
    #     sensor reading value.
    #     """
    #     self.assertTrue(False)
