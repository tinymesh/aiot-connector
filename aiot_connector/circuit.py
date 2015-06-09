# coding: utf-8
import dateutil.parser

def context_from_json_data(json_data):
    proto = json_data['proto/tm']

    return {
        'timestamp': dateutil.parser.parse(json_data['datetime']),
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

    print 'save_kwm', kwm1, 'and', kwm2

    if kwm2:
        kwm_avg = (kwm1 + kwm2) / 2.0
    else:
        kwm_avg = kwm1

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

