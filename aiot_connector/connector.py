# coding: utf-8
import json
import psycopg2
import requests

import building
import circuit
import settings


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


def process_json(cur, json_data):
    device = get_device_from_selector(cur, json_data['selector'])
    if device is None:
        device = create_device_from_selector(cur, json_data['selector'])

    if device['type'] == 'building-sensor-v2':
        context = building.context_from_json_data(json_data)
        building.save_sensor_data(cur, device, context)
        building.save_persons_inside(cur, device, context)
        building.save_deviations(cur, device, context)
    elif device['type'] == 'power-meter':
        context = circuit.context_from_json_data(json_data)
        circuit.save_pulses(cur, device, context)
        circuit.save_kwm(cur, device, context)


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

            json_data = json.loads(line[6:])
            process_json(cur, json_data)


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
