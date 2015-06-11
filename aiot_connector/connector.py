# coding: utf-8
import json
import psycopg2
import requests
from psycopg2.extras import DictCursor

from building import BuildingProcessor
from circuit import CircuitProcessor
import settings


class Connector:
    def __init__(self, conn):
        self.conn = conn
        self.cur = conn.cursor(cursor_factory=DictCursor)

    def get_device_from_selector(self, selector):
        network_key, device_key = selector
        self.cur.execute('SELECT key, type FROM device WHERE key = %(device_key)s', {
            'device_key': device_key,
        })
        return self.cur.fetchone()

    def create_device_from_selector(self, selector):
        network_key, device_key = selector

        url = 'https://http.cloud.tiny-mesh.com/v1/device/%s/%s' % (network_key, device_key)
        req = requests.get(url, auth=(settings.TM_USERNAME, settings.TM_PASSWORD), stream=True)
        device_data = req.json()

        self.cur.execute('INSERT INTO device (key, type, name) VALUES (%(key)s, %(type)s, %(name)s)', {
            'key': device_data['key'],
            'type': device_data['type'],
            'name': device_data.get('name', None),
        })

        device = self.get_device_from_selector(selector)
        assert device
        return device

    def process_json(self, json_data):
        device = self.get_device_from_selector(json_data['selector'])
        if device is None:
            device = self.create_device_from_selector(json_data['selector'])

        processor_map = {
            'building-sensor-v2': BuildingProcessor,
            'power-meter': CircuitProcessor,
        }

        processor_class = processor_map.get(device['type'], None)
        if processor_class:
            processor = processor_class(self.cur, device, json_data)
            processor.process()

    def loop(self):
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
                self.process_json(json_data)


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

    connector = Connector(conn)
    connector.loop()


if __name__ == '__main__':
    main()
