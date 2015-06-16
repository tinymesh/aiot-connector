# coding: utf-8
from dateutil import parser, rrule
from datetime import datetime, timedelta

from pytz import timezone

import settings

def trunc_datetime_to_hours(datetime):
    return datetime.replace(minute=0, second=0, microsecond=0)

class CircuitProcessor:
    def __init__(self, cur, device, json_data):
        self.cur = cur
        self.device = device

        proto = json_data['proto/tm']

        self.timestamp = parser.parse(json_data['datetime'])
        self.pulses = proto['msg_data']
        self.packet_number = proto['packet_number']

    def process(self):
        self.save_pulses()
        self.save_kwm()
        self.generate_kwh()

    ## Pulses

    def save_pulses(self):
        self.cur.execute("""
            INSERT INTO
                ts_pulses
                (datetime, device_key, value, packet_number)
            VALUES
                (%(datetime)s, %(device_key)s, %(value)s, %(packet_number)s)
        """, {
            'datetime': self.timestamp,
            'device_key': self.device['key'],
            'value': self.pulses,
            'packet_number': self.packet_number,
        })


    ## kWm

    def _get_kwm_from_two_pulses(self, packet_number):
        sql = """
            SELECT datetime, packet_number, value
            FROM ts_pulses
            WHERE packet_number in (%(packet_number_1)s, %(packet_number_2)s)
            AND device_key = %(device_key)s
            AND datetime > (NOW() - interval '1 day')
            ORDER BY datetime DESC
        """
        data = {
            'packet_number_1': packet_number,
            'packet_number_2': (packet_number - 1) % 2**16,
            'device_key': self.device['key']
        }
        self.cur.execute(sql, data)
        last_pulses = self.cur.fetchall()

        if not last_pulses or last_pulses[0]['packet_number'] != packet_number:
            return

        if len(last_pulses) == 1:
            return last_pulses[0]['value'] / 10000.0
        else:
            seconds_diff = (last_pulses[0]['datetime'] - last_pulses[1]['datetime']).total_seconds()
            multiplier = 60. / seconds_diff

            calibration_factor = 10000.0
            return last_pulses[0]['value'] * multiplier / calibration_factor

    def save_kwm(self):
        kwm1 = self._get_kwm_from_two_pulses(self.packet_number)
        kwm2 = self._get_kwm_from_two_pulses((self.packet_number - 1) % 2**16)

        if kwm2:
            kwm_avg = (kwm1 + kwm2) / 2.0
        else:
            kwm_avg = kwm1

        self.cur.execute('INSERT INTO ts_kwm (datetime, device_key, value) VALUES (%(datetime)s, %(device_key)s, %(value)s)', {
            'datetime': self.timestamp,
            'device_key': self.device['key'],
            'value': kwm_avg,
        })


    ## kWh

    def _get_first_kwm_timestamp_for_device(self):
        self.cur.execute('SELECT min(datetime) FROM ts_kwm WHERE device_key = %(device_key)s', {
            'device_key': self.device['key'],
        })
        return self.cur.fetchone()['min']

    def _get_last_kwh_timestamp_for_device(self):
        self.cur.execute('SELECT max(datetime) FROM ts_kwh WHERE device_key = %(device_key)s', {
            'device_key': self.device['key'],
        })
        return self.cur.fetchone()['max']

    def generate_kwh(self):
        oslo_tz = timezone('Europe/Oslo')

        last_kwh_timestamp = self._get_last_kwh_timestamp_for_device()

        if last_kwh_timestamp is not None:
            first_hour_to_check = trunc_datetime_to_hours(last_kwh_timestamp + timedelta(hours=1))
        else:
            first_kwm_timestamp = self._get_first_kwm_timestamp_for_device()
            if first_kwm_timestamp is not None:
                first_hour_to_check = trunc_datetime_to_hours(first_kwm_timestamp)
            else:
                return

        last_hour = trunc_datetime_to_hours(datetime.now(oslo_tz) - timedelta(hours=1))
        for hour_dt in rrule.rrule(rrule.HOURLY, dtstart=first_hour_to_check, until=last_hour):
            self.save_kwh(hour_dt)

    def save_kwh(self, hour_to_check):
        self.cur.execute("""
                SELECT
                    (SUM(value) / COUNT(value) * 60.0) AS kwh_scaled,
                    COUNT(value) AS num_kwm_measurements
                FROM
                    ts_kwm
                WHERE
                    datetime BETWEEN %(dt_start)s AND %(dt_end)s
                AND
                    device_key = %(device_key)s
            """, {
                'dt_start': hour_to_check,
                'dt_end': hour_to_check + timedelta(hours=1),
                'device_key': self.device['key'],
            }
        )
        result = self.cur.fetchone()

        if result['num_kwm_measurements'] < 30:
            if settings.DEBUG:
                print '%d measurements for device %s at hour %s in kwm timeseries (30 required)' % (
                        result['num_kwm_measurements'], self.device['key'], hour_to_check)
            return

        self.cur.execute('INSERT INTO ts_kwh (datetime, device_key, value) VALUES (%(datetime)s, %(device_key)s, %(value)s)', {
            'datetime': str(hour_to_check),
            'device_key': self.device['key'],
            'value': result['kwh_scaled'],
        })
