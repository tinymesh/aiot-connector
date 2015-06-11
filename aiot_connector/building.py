# coding: utf-8
import dateutil.parser
import math

class BuildingProcessor:
    def __init__(self, cur, device, json_data):
        self.cur = cur
        self.device = device

        # get relevant data from json
        proto = json_data['proto/tm']
        self.sensor_data = {
            'temp': (((((proto['locator'] & 65535) / 4.0) / 16382.0) * 165.0) - 40.0),
            'co2': proto['msg_data'],
            'light': pow(proto['analog_io_0'] * 0.0015658, 10),
            'moist': ((proto['locator'] >> 16) / 16382.0) * 100.0,
            'movement': bool(proto['digital_io_5']),
            'decibel': 90.0 - (30.0 * (proto['analog_io_1'] / 2048.0)),
        }
        self.timestamp = dateutil.parser.parse(json_data['datetime'])
        self.packet_number = proto['packet_number']

    def process(self):
        self.save_sensor_data()
        self.save_persons_inside()
        self.save_deviations()

    def save_sensor_data(self):
        type_to_table_name = {
            'temp': 'ts_temperature',
            'co2': 'ts_co2',
            'light': 'ts_light',
            'moist': 'ts_moist',
            'movement': 'ts_movement',
            'decibel': 'ts_decibel',
        }

        for type, value in self.sensor_data.items():
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
        current_co2 = self.sensor_data['co2']

        self.cur.execute("""
            SELECT (%(current_co2)s - MIN(value)) / STDDEV(value) AS value
            FROM ts_co2
            WHERE device_key = %(device_key)s
            AND value BETWEEN 50 AND 8000
        """, {
            'device_key': self.device['key'],
            'current_co2': current_co2,
        })
        num_persons_inside = math.trunc(self.cur.fetchone()['value'] - 0.2)

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

    def save_deviations(self):
        deviation_type = None

        if not (100 < self.sensor_data['co2'] < 200):
            deviation_type = 'co2'
        elif not (20 < self.sensor_data['moist'] < 60):
            deviation_type = 'moist'
        elif not (19 < self.sensor_data['temperature'] < 23):
            deviation_type = 'temperature'

        if deviation_type:
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
