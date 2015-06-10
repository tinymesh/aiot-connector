# coding: utf-8
from dateutil import parser, rrule
from datetime import datetime, timedelta

from pytz import timezone
import pytz

import settings

def context_from_json_data(json_data):
    proto = json_data['proto/tm']

    return {
        'timestamp': parser.parse(json_data['datetime']),
        'pulses': proto['msg_data'],
        'packet_number': proto['packet_number'],
    }

def save_pulses(cur, device, context):
    sql = """
        INSERT INTO
            ts_pulses
            (datetime, device_key, value, packet_number)
        VALUES
            (%(datetime)s, %(device_key)s, %(value)s, %(packet_number)s)
    """
    data = {
            'datetime': context['timestamp'],
            'device_key': device['key'],
            'value': context['pulses'],
            'packet_number': context['packet_number'],
    }

    cur.execute(sql, data)

def get_kwm_from_two_pulses(cur, device, packet_number):
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
        'device_key': device['key']
    }
    cur.execute(sql, data)
    last_pulses = cur.fetchall()

    if not last_pulses or last_pulses[0][1] != packet_number:
        return

    if settings.DEBUG:
        print 'get_kwm_from_two_pulses', last_pulses

    if len(last_pulses) == 1:
        return last_pulses[0][2] / 10000.0
    else:
        seconds_diff = (last_pulses[0][0] - last_pulses[1][0]).total_seconds()
        multiplier = 60. / seconds_diff #TODO Check why diff if zero sometimes

        calibration_factor = 10000.0
        return last_pulses[0][2] * multiplier / calibration_factor

def save_kwm(cur, device, context):
    kwm1 = get_kwm_from_two_pulses(cur, device, context['packet_number'])
    kwm2 = get_kwm_from_two_pulses(cur, device, (context['packet_number'] - 1) % 2**16)

    if settings.DEBUG:
        print 'save_kwm', kwm1, 'and', kwm2

    if kwm2:
        kwm_avg = (kwm1 + kwm2) / 2.0
    else:
        kwm_avg = kwm1

    if settings.DEBUG:
        print 'avg is', kwm_avg

    sql = """
        INSERT INTO
            ts_kwm
            (datetime, device_key, value)
        VALUES
            (%(datetime)s, %(device_key)s, %(value)s)
    """
    data = {
        'datetime': context['timestamp'],
        'device_key': device['key'],
        'value': kwm_avg,
    }
    cur.execute(sql, data)

def trunc_datetime_to_hours(datetime):
    return datetime.replace(minute=0, second=0, microsecond=0)

def get_first_kwm_timestamp_for_device(cur, device):
    cur.execute("""
            SELECT min(datetime)
            FROM ts_kwm
            WHERE
                device_key = %(device_key)s
        """,
        {'device_key': device['key']}
    )
    return cur.fetchone()[0]

def get_last_kwh_timestamp_for_device(cur, device):
    cur.execute("""
            SELECT max(datetime)
            FROM ts_kwh
            WHERE
                device_key = %(device_key)s
        """,
        {'device_key': device['key']}
    )
    return cur.fetchone()[0]

def kwh_measurement_exists(cur, device, hour_dt):
    cur.execute("""
            SELECT *
            FROM ts_kwh
            WHERE
                device_key = %(device_key)s
                AND datetime = %(datetime)s
        """,
        {
            'device_key': device['key'],
            'datetime': hour_dt
        }
    )
    return bool(cur.fetchall())


def generate_kwh(cur, device):
    oslo_tz = timezone('Europe/Oslo')

    last_hour = trunc_datetime_to_hours(datetime.now(oslo_tz) - timedelta(hours=1))
    last_kwh_timestamp = get_last_kwh_timestamp_for_device(cur, device)

    if last_kwh_timestamp is not None:
        first_hour_to_check = trunc_datetime_to_hours(last_kwh_timestamp)
    else:
        first_kwm_timestamp = get_first_kwm_timestamp_for_device(cur, device)
        if first_kwm_timestamp is not None:
            first_hour_to_check = trunc_datetime_to_hours(first_kwm_timestamp)
        else:
            return

    for hour_dt in rrule.rrule(rrule.HOURLY, dtstart=first_hour_to_check, until=last_hour):
        if not kwh_measurement_exists(cur, device, hour_dt):
            save_kwh(cur, device, hour_dt)
        else:
            if settings.DEBUG:
                print 'Measurement for device %s at hour %s in kwm timeseries already exists' % (device['key'], hour_dt)

def save_kwh(cur, device, hour_to_check):
    cur.execute("""
            SELECT value
            FROM ts_kwm 
            WHERE 
                datetime BETWEEN %(dt_start)s AND %(dt_end)s
                AND device_key = %(device_key)s
        """,
        {
            'dt_start': hour_to_check,
            'dt_end': hour_to_check + timedelta(hours=1),
            'device_key': device['key']
        }
    )
    res = cur.fetchall()

    number_of_kwm_measurements = len(res)

    if number_of_kwm_measurements < 30:
        if settings.DEBUG:
            print '%d measurements for device %s at hour %s in kwm timeseries (30 required)' % (number_of_kwm_measurements, device['key'], hour_to_check)
        return

    sum = 0
    for tuple in res:
        sum += tuple[0]

    value = sum / number_of_kwm_measurements

    cur.execute("""
            INSERT INTO
                ts_kwh (datetime, device_key, value)
            VALUES 
                (%(datetime)s, %(device_key)s, %(value)s)
        """,
        {
            'datetime': str(hour_to_check),
            'device_key': device['key'],
            'value': value,

        }
    )
