# coding: utf-8
import dateutil.parser

def invert_endianess(n):
    a = (n >> 24) & 0xFF
    b = (n >> 16) & 0xFF
    c = (n >> 8) & 0xFF
    d = (n >> 0) & 0xFF
    return a ^ (b << 8) ^ (c << 16) ^ (d << 24)

class WristbandProcessor:
    def __init__(self, cur, device, json_data):
        self.cur = cur
        self.device = device

        # get relevant data from json
        proto = json_data['proto/tm']

        self.timestamp = dateutil.parser.parse(json_data['datetime'])
        self.packet_number = proto['packet_number']
        self.button_was_pushed = proto['detail'] == 'io_change'

        if self.button_was_pushed:
            self.rssi = None
            self.nearest_uid = None
            self.nearest_device_key = None
        else:
            self.rssi = proto['data'] >> 8
            self.nearest_uid = invert_endianess(proto['locator'])
            self.nearest_device_key = self._get_device_key_from_uid(self.nearest_uid)

    def _get_device_key_from_uid(self, uid):
        self.cur.execute("SELECT key FROM device WHERE uid = %(uid)s", {
            'uid': uid,
        })

        ret = self.cur.fetchone()
        if ret:
            return ret['key']

    def process(self):
        print
        print '*' * 80
        print '*', vars(self), '*'
        print '*' * 80
        print

        if self.button_was_pushed:
            self.save_wristband_button_push()

        if self.nearest_device_key:
            self.save_wristband_location()

    def save_wristband_location(self):
        self.cur.execute("""
            INSERT INTO
                ts_wristband_location
                (device_key, datetime, nearest_device_key, rssi, packet_number)
            VALUES
                (%(device_key)s, %(timestamp)s, %(nearest_device_key)s, %(rssi)s, %(packet_number)s)
        """, {
            'device_key': self.device['key'],
            'timestamp': self.timestamp,
            'nearest_device_key': self.nearest_device_key,
            'rssi': self.rssi,
            'packet_number': self.packet_number,
        })

    def save_wristband_button_push(self):
        self.cur.execute("""
            INSERT INTO
                ts_wristband_button_push
                (device_key, datetime, packet_number)
            VALUES
                (%(device_key)s, %(timestamp)s, %(packet_number)s)
        """, {
            'device_key': self.device['key'],
            'timestamp': self.timestamp,
            'packet_number': self.packet_number,
        })
