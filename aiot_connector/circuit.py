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

def save_kwm(cur, device, context):
    # 1. hent ut siste måling (før denne) aka packet_number = this.packet_number - 1;
    # 2. trunc context['timestamp'] til siste minutt
    # 3. sjekk om time diff er 1. hvis ikke, gjør Det Riktige?
    # 4. push in kwm (bruke time diff som faktor)
    pass

def save_energy_productivity(cur, device, context):
    # 1. hent ut siste kwm + siste produktivitet
    # 2. del.
    # 3. push.
    pass
