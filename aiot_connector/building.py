# coding: utf-8
import dateutil.parser
import math
import random

def context_from_json_data(json_data):
    proto = json_data['proto/tm']

    return {
        'sensor_data': {
            'temp': (((((proto['locator'] & 65535) / 4.0) / 16382.0) * 165.0) - 40.0),
            'co2': proto['msg_data'],
            'light': pow(proto['analog_io_0'] * 0.0015658, 10),
            'moist': ((proto['locator'] >> 16) / 16382.0) * 100.0,
            'movement': bool(proto['digital_io_5']),
            'decibel': 90.0 - (30.0 * (proto['analog_io_1'] / 2048.0)),
        },
        'timestamp': dateutil.parser.parse(json_data['datetime']),
        'packet_number': proto['packet_number'],
    }

def save_sensor_data(cur, device, context):
    type_to_table_name = {
        'temp': 'ts_temperature',
        'co2': 'ts_co2',
        'light': 'ts_light',
        'moist': 'ts_moist',
        'movement': 'ts_movement',
        'decibel': 'ts_decibel',
    }

    for type, value in context['sensor_data'].items():
        table_name = type_to_table_name[type]

        sql = """
            INSERT INTO
                """ + table_name + """
                (datetime, device_key, value, packet_number)
            VALUES (
                %(datetime)s,
                %(device_key)s,
                %(value)s,
                %(packet_number)s
            )
        """
        data = {
            'datetime': context['timestamp'],
            'device_key': device['key'],
            'value': value,
            'packet_number': context['packet_number'],
        }

        cur.execute(sql, data)

def save_persons_inside(cur, device, context):
    current_co2 = context['sensor_data']['co2']

    cur.execute("""
        SELECT (%(current_co2)s - MIN(value)) / STDDEV(value)
        FROM ts_co2
        WHERE device_key = %(device_key)s
        AND value BETWEEN 50 AND 8000
    """, {
        'device_key': device['key'],
        'current_co2': current_co2,
    })
    num_persons_inside = math.trunc(cur.fetchone()[0] - 0.2)

    # Clamp between 0 and 15
    num_persons_inside = min(num_persons_inside, 15)
    num_persons_inside = max(num_persons_inside, 0)

    cur.execute("""
        INSERT INTO
            ts_persons_inside
            (datetime, device_key, value)
        VALUES
            (%(timestamp)s, %(device_key)s, %(value)s)
    """, {
        'timestamp': context['timestamp'],
        'device_key': device['key'],
        'value': num_persons_inside,
    })

def save_deviations(cur, device, context):
    sensor_data = context['sensor_data']

    deviation_type = None

    if not (100 < sensor_data['co2'] < 200):
        deviation_type = 'co2'
    elif not (20 < sensor_data['moist'] < 60):
        deviation_type = 'moist'
    elif not (19 < sensor_data['temperature'] < 23):
        deviation_type = 'temperature'

    if deviation_type:
        cur.execute("""
            INSERT INTO
                deviations
                (datetime, device_key, deviation_type)
            VALUES
                (%(datetime)s, %(device_key)s, %(deviation_type)s)
        """, {
            'datetime': context['timestamp'],
            'device_key': device['key'],
            'deviation_type': deviation_type,
        })

