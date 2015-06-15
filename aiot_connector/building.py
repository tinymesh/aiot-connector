# coding: utf-8
import dateutil.parser
import math
import psycopg2
from random import randint

from pytz import timezone
from datetime import timedelta, datetime

import settings

def trunc_datetime_to_minutes(datetime):
    return datetime.replace(second=0, microsecond=0)

class BuildingProcessor:
    def __init__(self, cur, device, json_data):
        self.cur = cur
        self.device = device

        # get relevant data from json
        proto = json_data['proto/tm']
        self.sensor_data = {
            'temperature': (((((proto['locator'] & 65535) / 4.0) / 16382.0) * 165.0) - 40.0),
            'co2': proto['msg_data'],
            'light': pow(proto['analog_io_0'] * 0.0015658, 10),
            'moist': ((proto['locator'] >> 16) / 16382.0) * 100.0,
            'movement': bool(proto['digital_io_5']),
            'decibel': 90.0 - (30.0 * (proto['analog_io_1'] / 2048.0)),
        }
        self.timestamp = dateutil.parser.parse(json_data['datetime'])
        self.packet_number = proto['packet_number']

        # filter out "bad" values
        if self.sensor_data['temperature'] < 0:
            self.sensor_data['temperature'] = None

        if not (100 < self.sensor_data['co2'] < 8000):
            self.sensor_data['co2'] = None

        if self.sensor_data['moist'] == 0:
            self.sensor_data['moist'] = None

    def process(self):
        self.save_sensor_data()
        self.save_persons_inside()
        self.save_subjective_evaluation()
        self.save_deviations()
        self.save_energy_productivity()

    def save_subjective_evaluation(self):
        self.cur.execute("""
            SELECT value
            FROM ts_movement
            WHERE device_key = %(device_key)s
            ORDER BY datetime DESC
            LIMIT 2
            """,
            {
                'device_key': self.device['key']
            }
        )

        res = self.cur.fetchall()

        if len(res) != 2 or not (not res[0] and res[1]):
            return


        self.cur.execute("""
                INSERT INTO ts_subjective_evaluation (datetime, value, device_key)
                VALUES (%(datetime)s, %(value)s, %(device_key)s)
            """,
            {
                'datetime': self.timestamp,
                'value': randint(-1, 1), # Fake values for now..
                'device_key': self.device['key']
            }
        )


    #TODO: Refactor me
    def save_energy_productivity(self):
        self.cur.execute("""
                SELECT *
                FROM ts_room_productivity
                WHERE device_key = %(device_key)s
                AND datetime =
                    (SELECT MAX(datetime)
                    FROM ts_room_productivity
                    WHERE device_key = %(device_key)s)
            """,
            {
                'device_key': self.device['key']
            }
        )

        last_room_productivity = self.cur.fetchone()
        if last_room_productivity is None:
            if settings.DEBUG:
                print 'Could not find room_productivity for device %s' % self.device['key']
            return
        last_room_productivity['datetime'] = trunc_datetime_to_minutes(last_room_productivity['datetime'])

        self.cur.execute("""
            SELECT *
            FROM ts_energy_productivity
            WHERE datetime = %(datetime)s
                AND device_key = %(device_key)s
            """,
            {
                'datetime': last_room_productivity['datetime'],
                'device_key': self.device['key']
            }
        )

        if self.cur.fetchone():
            if settings.DEBUG:
                print 'Energy productivity for device %s at minute %s already exists' % (self.device['key'],
                        last_room_productivity['datetime'])
            return


        self.cur.execute("""
                SELECT device_key
                FROM map_device_power_circuit
            """,
            {}
        )

        power_circuit_device_keys = self.cur.fetchall()
        oslo_tz = timezone('Europe/Oslo')

        total_energy_consumption = 0
        for power_circuit_device in power_circuit_device_keys:
            self.cur.execute("""
                SELECT AVG(value)
                FROM ts_kwm
                WHERE datetime >= %(five_minutes_ago)s
                    AND device_key = %(device_key)s
            """,
            {
                'five_minutes_ago': datetime.now(oslo_tz) - timedelta(minutes=5),
                'device_key': power_circuit_device[0]
            }
            )
            res = self.cur.fetchone()
            if res[0] is None:
                if settings.DEBUG:
                    print 'Could not find kwm values within the last five minutes on device %s' % self.device['key']
                return
            total_energy_consumption += res[0]


        total_area = 15000

        self.cur.execute("""
                SELECT area
                FROM room
                WHERE key = (SELECT room_key FROM map_device_room WHERE device_key = %(device_key)s)
            """,
            {
                'device_key': self.device['key']
            }
        )

        area = float(self.cur.fetchone()[0])

        area_factor = total_area / area

        value = last_room_productivity['value'] / total_energy_consumption * area_factor

        self.cur.execute("""
                INSERT INTO ts_energy_productivity (datetime, device_key, value)
                VALUES (%(datetime)s, %(device_key)s, %(value)s)
            """,
            {
                'datetime': last_room_productivity['datetime'],
                'device_key': self.device['key'],
                'value': value

            }

        )

    def save_sensor_data(self):
        type_to_table_name = {
            'temperature': 'ts_temperature',
            'co2': 'ts_co2',
            'light': 'ts_light',
            'moist': 'ts_moist',
            'movement': 'ts_movement',
            'decibel': 'ts_decibel',
        }

        for type, value in self.sensor_data.items():
            # Might be a filtered-out value
            if value is None:
                continue

            table_name = type_to_table_name[type]

            self.cur.execute("""
                INSERT INTO
                    """ + table_name + """
                    (datetime, device_key, value, packet_number)
                VALUES (
                    %(datetime)s,
                    %(device_key)s,
                    %(value)s,
                    %(packet_number)s
                )
            """, {
                'datetime': self.timestamp,
                'device_key': self.device['key'],
                'value': value,
                'packet_number': self.packet_number,
            })

    def save_persons_inside(self):
        # Might be a filtered-out value
        if self.sensor_data['co2'] is None:
            return

        current_co2 = self.sensor_data['co2']
        print current_co2, self.device['key']

        try:
            self.cur.execute("""
                SELECT (%(current_co2)s - MIN(value)) / STDDEV(value) AS value
                FROM ts_co2
                WHERE device_key = %(device_key)s
                AND value BETWEEN 50 AND 8000
            """, {
                'device_key': self.device['key'],
                'current_co2': current_co2,
            })
        except psycopg2.DataError:
            # Catches a division by zero error if current_co2 == MIN(value)
            return

        obj = self.cur.fetchone()
        if not obj or not obj[0]:
            return
        num_persons_inside = math.trunc(obj['value'] - 0.2)

        # Clamp between 0 and 15
        num_persons_inside = min(num_persons_inside, 15)
        num_persons_inside = max(num_persons_inside, 0)

        self.cur.execute("""
            INSERT INTO
                ts_persons_inside
                (datetime, device_key, value)
            VALUES
                (%(timestamp)s, %(device_key)s, %(value)s)
        """, {
            'timestamp': self.timestamp,
            'device_key': self.device['key'],
            'value': num_persons_inside,
        })

    def add_deviation(self, deviation_type):
        self.cur.execute("""
            INSERT INTO
                deviations
                (datetime, device_key, deviation_type)
            VALUES
                (%(datetime)s, %(device_key)s, %(deviation_type)s)
        """, {
            'datetime': self.timestamp,
            'device_key': self.device['key'],
            'deviation_type': deviation_type,
        })

    def save_deviations(self):
        if self.sensor_data['co2'] is not None:
            if not (100 < self.sensor_data['co2'] < 200):
                self.add_deviation('co2')

        if self.sensor_data['moist'] is not None:
            if not (20 < self.sensor_data['moist'] < 60):
                self.add_deviation('moist')

        if self.sensor_data['temperature'] is not None:
            if not (19 < self.sensor_data['temperature'] < 23):
                self.add_deviation('temperature')
