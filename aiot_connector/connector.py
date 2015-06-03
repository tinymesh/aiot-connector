# -*- coding: utf-8 -*-
import dateutil.parser
import json
import psycopg2
import requests

import settings


def save_room_state_measures(cur, device, timestamp, sensor_data, packet_number):
    type_to_table_name = {
        'temp': 'ts_temperature',
        'co2': 'ts_co2',
        'light': 'ts_light',
        'moist': 'ts_moist',
        'movement': 'ts_movement',
        'noise': 'ts_noise',
    }

    for type, value in sensor_data.items():
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
            'datetime': timestamp,
            'device_key': device['key'],
            'value': value,
            'packet_number': packet_number,
        }

        cur.execute(sql, data)

def save_power_measure(cur, device, timestamp, sensor_data, packet_number):
    sql = """
        INSERT INTO
            ts_pulses
            (datetime, device_key, value, packet_number)
        VALUES
            (%(datetime)s, %(device_key)s, %(value)s, %(packet_number)s)
    """
    data = {
            'datetime': timestamp,
            'device_key': device['key'],
            'value': sensor_data['pulses'],
            'packet_number': packet_number,
    }

    cur.execute(sql, data)

def get_device_from_selector(cur, selector):
    network_key, device_key = selector
    cur.execute('SELECT key, type FROM device WHERE key = %(device_key)s', {
        'device_key': device_key,
    })
    ret = cur.fetchone()

    if ret:
        return {
            'key': ret[0],
            'type': ret[1],
        }


def create_device_from_selector(cur, selector):
    network_key, device_key = selector

    url = 'https://http.cloud.tiny-mesh.com/v1/device/%s/%s' % (network_key, device_key)
    req = requests.get(url, auth=(settings.TM_USERNAME, settings.TM_PASSWORD), stream=True)
    device_data = req.json()

    cur.execute("""
        INSERT INTO
            device
            (key, type, name)
        VALUES
            (%(key)s, %(type)s, %(name)s)
    """, {
        'key': device_data['key'],
        'type': device_data['type'],
        'name': device_data.get('name', None),
    })

    device = get_device_from_selector(cur, selector)
    assert device
    return device


def process_building_sensor_data(cur, device, timestamp, data):
    proto = data['proto/tm']
    sensor_data = {
        'temp': (((((proto['locator'] & 65535) / 4.0) / 16382.0) * 165.0) - 40.0),
        'co2': proto['msg_data'],
        'light': pow(proto['analog_io_0'] * 0.0015658, 10),
        'moist': ((proto['locator'] >> 16) / 16382.0) * 100.0,
        'movement': bool(proto['digital_io_5']),
        'noise': 90.0 - (30.0 * (proto['analog_io_1'] / 2048.0)),
    }

    packet_number = proto['packet_number']

    save_room_state_measures(cur, device, timestamp, sensor_data, packet_number)
    return True


def process_power_meter_data(cur, device, timestamp, data):
    proto = data['proto/tm']
    sensor_data = {
        'pulses': proto['msg_data'],
    }
    packet_number = proto['packet_number']

    save_power_measure(cur, device, timestamp, sensor_data, packet_number)
    return True


def process_json(cur, data):
    device = get_device_from_selector(cur, data['selector'])
    if device is None:
        device = create_device_from_selector(cur, data['selector'])

    sensor_map = {
        'building-sensor-v2': process_building_sensor_data,
        'power-meter': process_power_meter_data,
    }

    process_fn = sensor_map.get(device['type'], None)

    if process_fn:
        if settings.DEBUG:
            print 'Using %s to process data' % process_fn.__name__
        timestamp = dateutil.parser.parse(data['datetime'])
        return process_fn(cur, device, timestamp, data)


def start_loop(cur):
    url = 'https://http.cloud.tiny-mesh.com/v1/message-query/%s/?stream=stream/%s&query=proto/tm.type:event' % (
        settings.TM_NETWORK,
        settings.TM_NETWORK,
    )
    req = requests.get(url, auth=(settings.TM_USERNAME, settings.TM_PASSWORD), stream=True)

    for line in req.iter_lines():
        if line:
            if not line.startswith('data: '):
                continue

            if settings.DEBUG:
                print '-' * 80
                print line

            data = json.loads(line[6:])
            process_json(cur, data)


def main():
    psycopg_kwargs = {
        'database': settings.DB_NAME,
        'user': settings.DB_USER,
    }

    # Makes it possible to set it to `None` if you want to connect using
    # local unix sockets.
    if settings.DB_HOST:
        psycopg_kwargs['host'] = settings.DB_HOST

    if settings.DB_PASSWORD:
        psycopg_kwargs['password'] = settings.DB_PASSWORD

    conn = psycopg2.connect(**psycopg_kwargs)
    conn.autocommit = True

    cur = conn.cursor()
    start_loop(cur)


if __name__ == '__main__':
    main()
