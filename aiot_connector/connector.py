# coding: utf-8
import json
import psycopg2
import requests
from psycopg2.extras import DictCursor

from building import BuildingProcessor
from circuit import CircuitProcessor
from wristband import WristbandProcessor

import settings


class Connector:
    def __init__(self, conn):
        self.conn = conn
        self.cur = conn.cursor(cursor_factory=DictCursor)

    def _get_device_from_api(self, selector):
        network_key, device_key = selector
        url = 'https://http.cloud.tiny-mesh.com/v1/device/%s/%s' % (network_key, device_key)
        req = requests.get(url, auth=(settings.TM_USERNAME, settings.TM_PASSWORD), stream=True)
        return req.json()

    def update_device_from_selector(self, selector):
        device_data = self._get_device_from_api(selector)

        self.cur.execute('UPDATE device SET type = %(type)s, name = %(name)s, uid = %(uid)s WHERE key = %(key)s', {
            'key': device_data['key'].encode('utf-8'),
            'type': device_data['type'].encode('utf-8'),
            'name': device_data.get('name', u'N/A').encode('utf-8'),
            'uid': device_data['address']
        })

    def create_device_from_selector(self, selector):
        device_data = self._get_device_from_api(selector)
        if settings.DEBUG:
            print
            print '** creating device'
            print device_data
            print

        self.cur.execute('INSERT INTO device (key, type, name, uid) VALUES (%(key)s, %(type)s, %(name)s, %(uid)s)', {
            'key': device_data['key'].encode('utf-8'),
            'type': device_data['type'].encode('utf-8'),
            'name': device_data.get('name', u'N/A').encode('utf-8'),
            'uid': device_data['address']
        })

        device = self.get_device_from_selector(selector)
        assert device
        return device

    def get_device_from_selector(self, selector):
        network_key, device_key = selector
        self.cur.execute('SELECT key, type FROM device WHERE key = %(device_key)s', {
            'device_key': device_key,
        })
        return self.cur.fetchone()

    def process_json(self, json_data):
        device = self.get_device_from_selector(json_data['selector'])
        if device is None:
            device = self.create_device_from_selector(json_data['selector'])
        elif settings.UPDATE_DEVICES:
            self.update_device_from_selector(json_data['selector'])

        processor_map = {
            'building-sensor-v2': BuildingProcessor,
            'power-meter': CircuitProcessor,
            'wristband': WristbandProcessor,
        }

        print '- device.type:', device['type']
        print '- payload.detail:', json_data['proto/tm']['detail']
        print '- payload.type:', json_data['proto/tm']['type']
        processor_class = processor_map.get(device['type'], None)
        if processor_class:
            processor = processor_class(self, device, json_data)
            processor.process()

    def do_hook(self, hook_name, processor):
        if hook_name in settings.HOOKS:
            settings.HOOKS[hook_name](self, processor)

    def loop(self):
        url = 'https://http.cloud.tiny-mesh.com/v1/message-query/%s/?stream=stream/%s&query=proto/tm.type:event&data-encoding=binary' % (
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
