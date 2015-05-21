# -*- coding: utf-8 -*-
import dateutil.parser
import json
import psycopg2
import requests

import settings


def save_room_state(cur, device, timestamp, sensor_data):
    sql = """
        INSERT INTO room_state
        values (
            %(timestamp)s,
            %(room_key)s,
            %(co2)s,
            %(noise)s,
            %(movement)s,
            %(temp)s,
            %(moist)s,
            %(light)s,
            DEFAULT
        )
    """
    data = {
        'room_key': device['room'],
        'timestamp': timestamp,
        'co2': sensor_data['co2'],
        'noise': sensor_data['noise'],
        'movement': sensor_data['movement'],
        'temp': sensor_data['temp'],
        'moist': sensor_data['moist'],
        'light': sensor_data['light'],
    }

    cur.execute(sql, data)

def save_power_measure(cur, device, timestamp, sensor_data):
    sql = """
        INSERT INTO
            power_meter_timeseries
            (device_key, datetime, pulses)
        VALUES
            (%(device_key)s, %(timestamp)s, %(pulses)s)
    """
    data = {
        'device_key': device['key'],
        'timestamp': timestamp,
        'pulses': sensor_data['pulses'],
    }

    cur.execute(sql, data)

def get_device_from_selector(cur, selector):
    network_key, device_key = selector
    cur.execute('SELECT key, room, type FROM device WHERE key = %(device_key)s', {
        'device_key': device_key,
    })
    ret = cur.fetchone()

    if ret:
        return {
            'key': ret[0],
            'room': ret[1],
            'type': ret[2],
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
    if device['room'] is None:
        print 'Found no room for selector %s' % '/'.join(data['selector'])
        return False

    proto = data['proto/tm']
    sensor_data = {
        'co2': proto['msg_data'],
        'light': pow(10, data['proto/tm']['analog_io_0'] * 0.0015658),
        'movement': bool(data['proto/tm']['digital_io_5']),
        'noise':  (data['proto/tm']['digital_io_1']/ 2048),
        'temp': ((((((data['proto/tm']['analog_io_1'] & 65535) / 4) / 16382) * 165) - 40) * 100) / 100,
        'moist': (data['proto/tm']['locator'] >> 16)
    }

    save_room_state(cur, device, timestamp, sensor_data)
    return True


def process_power_meter_data(cur, device, timestamp, data):
    proto = data['proto/tm']
    sensor_data = {
        'pulses': proto['msg_data'],
    }

    save_power_measure(cur, device, timestamp, sensor_data)
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
