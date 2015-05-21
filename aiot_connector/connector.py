# -*- coding: utf-8 -*-
import dateutil.parser
import json
import psycopg2
import requests

import settings


def save_message(cur, timestamp, device, sensor_data):
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

    if settings.DEBUG:
        cur.mogrify(sql, data)

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

    req = requests.get('https://http.cloud.tiny-mesh.com/v1/device/%s/%s' % (network_key, device_key), auth=(settings.TM_USERNAME, settings.TM_PASSWORD), stream=True)
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


def process_json(cur, data):
    timestamp = dateutil.parser.parse(data['datetime'])

    device = get_device_from_selector(cur, data['selector'])
    if device is None:
        device = create_device_from_selector(cur, data['selector'])

    if device['type'] == 'building-sensor-v2':
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

        #sensor_data = {
        #    'temp': (((((proto['locator'] & 65535) / 4) / 16382) * 165) - 40),
        #    'co2': proto['msg_data'],
        #    'light': pow((proto['analog_io_0'] * 0.0015658), 10),
        #    'moist': ((proto['locator'] >> 16),
        #    'movement': bool(proto['digital_io_5']),
        #    'noise': (90 - (30 * (proto['analog_io_1'] / 2048)))
        #}

        save_message(cur, timestamp, device, sensor_data)
        return True

    elif device['type'] == 'power-meter':
        # Helge fikser her
        print u'STRØM STRØM STRØØØØØØØØØØØØØØØØØØM!!!'

def start_loop(cur):
    req = requests.get('https://http.cloud.tiny-mesh.com/v1/message-query/%s/?stream=stream/%s&query=proto/tm.type:event' % (settings.TM_NETWORK, settings.TM_NETWORK),
                       auth=(settings.TM_USERNAME, settings.TM_PASSWORD), stream=True)

    for line in req.iter_lines():
        if line:
            if not line.startswith('data: '):
                continue

            if settings.DEBUG:
                print line

            data = json.loads(line[6:])
            success = process_json(cur, data)

            if success:
                print 'SUCCESS!'
            else:
                print 'FAIL :-('


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

    print psycopg_kwargs
    conn = psycopg2.connect(**psycopg_kwargs)
    conn.autocommit = True

    cur = conn.cursor()
    start_loop(cur)


if __name__ == '__main__':
    main()
