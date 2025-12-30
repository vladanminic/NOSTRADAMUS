# Swagger API documentation: https://nostradamus-ioto.issel.ee.auth.gr/api/docs
# Jupyter notebook with examples: https://colab.research.google.com/drive/1Uu12nIu1LhkTnb5Y-Sq1ZqeZkn3mjWNE?usp=sharing

import os
from pkgutil import get_data
import psycopg2
import json
import httpx
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta
import requests
import pytz

utc=pytz.UTC

# Configuration
BASE_URL = "https://nostradamus-ioto.issel.ee.auth.gr/api/v1"
PROJECT_ID = "6e2bcf44-f12a-4acd-853a-1468752785a8"

# API Keys
MASTER_KEY = 'CHANGE_ME'
WRITE_KEY = 'CHANGE_ME'
READ_KEY = 'd1091a7ac0fea235a022005637ed1f23dd7de7bf50ddf82d69bf68e724ecb65a'

# Station Configuration - centralized for easier use
STATION_CONFIG = {
    'RHMZ': {
        'prefix': '_RHMZ',
        'collection_name': 'station_type_1',
        'collection_id': None,  # Will be populated dynamically
        'excluded_modules': []
    },
    'PIS': {
        'prefix': 'PIS_',
        'collection_name': 'station_type_2',
        'collection_id': None,  # Will be populated dynamically
        'excluded_modules': [] 
        # 'excluded_modules': ['PIS_COKA', 'PIS_BACKI_VINOGRADI', 'PIS_BELA_CRKVA']
    },
}

# Database connection configuration
DB_CONFIG = {
    'dbname': 'CHANGE_ME',
    'user': 'CHANGE_ME',
    'password': 'CHANGE_ME',
    'host': 'CHANGE_ME',
    'port': 5432
}

# Date range for data fetching
date_from = "2022-01-01T00:00:00Z"
date_to = "2023-12-31T23:00:00Z"


def get_station_by_mac(mac_address):
    """Returns station type (PIS or RHMZ) based on MAC address"""
    if mac_address.startswith('PIS_'):
        return 'PIS'
    else:
        return 'RHMZ'


def get_collection_id_for_mac(mac_address):
    """Returns collection_id for given MAC address"""
    station_type = get_station_by_mac(mac_address)
    return STATION_CONFIG[station_type]['collection_id']


def is_module_excluded(mac_address):
    """Checks if module is excluded from processing"""
    station_type = get_station_by_mac(mac_address)
    return mac_address in STATION_CONFIG[station_type]['excluded_modules']


def get_pis_query(date_from, date_to, date_middle_1, date_middle_2):
    return '''
    -- PIS      
    WITH params AS (
        SELECT 
            %s::varchar AS mac_address,
            %s::timestamp AS date_from,
            %s::timestamp AS date_to,
            %s::timestamp AS date_middle_1,
            %s::timestamp AS date_middle_2
    ),
    module_info AS (
        SELECT
            m.mac_address,
            m.name AS module_name,
            ROUND(ST_Y(ml.location::geometry)::decimal, 5) AS latitude,
            ROUND(ST_X(ml.location::geometry)::decimal, 5) AS longitude
        FROM lora_module m
        JOIN lora_module_location ml ON m.mac_address = ml.mac_address_lora_module
        JOIN params p ON m.mac_address = p.mac_address
    ),
    raw_data AS (
        SELECT
            lm.date,
            lm.value,
            st.name AS sensor_name,
            lm.mac_address_lora_module
        FROM lora_measurement lm
        JOIN lora_device_type_sensor_type ldt ON lm.id_lora_device_type_sensor_type = ldt.id
        JOIN lora_sensor_type st ON ldt.id_lora_sensor_type = st.id
        JOIN params p ON lm.mac_address_lora_module = p.mac_address
        WHERE
            lm.device_on IS TRUE
            AND lm.valid IS TRUE
            AND (
                -- Oba middle datuma postoje - dva perioda
                (p.date_middle_1 IS NOT NULL AND p.date_middle_2 IS NOT NULL AND (
                    (lm.date > p.date_from AND lm.date < p.date_middle_1) OR
                    (lm.date > p.date_middle_2 AND lm.date < p.date_to)
                ))
                OR
                -- Samo date_middle_1 postoji - period od date_from do date_middle_1
                (p.date_middle_1 IS NOT NULL AND p.date_middle_2 IS NULL AND (
                    lm.date > p.date_from AND lm.date < p.date_middle_1
                ))
                OR
                -- Samo date_middle_2 postoji - period od date_middle_2 do date_to
                (p.date_middle_1 IS NULL AND p.date_middle_2 IS NOT NULL AND (
                    lm.date > p.date_middle_2 AND lm.date < p.date_to
                ))
                OR
                -- Nijedan middle datum ne postoji - ceo period
                (p.date_middle_1 IS NULL AND p.date_middle_2 IS NULL AND (
                    lm.date > p.date_from AND lm.date < p.date_to
                ))
            )
            AND st.name IN (
                'Temperatura vazduha',
                'Vlažnost vazduha',
                'Količina padavina',
                'Tačka rose',
                'Vlažnost lista'
            )
    )
    SELECT
        (jsonb_build_object(
            'key', mi.mac_address,
            'name', mi.module_name,
            'timestamp', rd.date,
            'air-temperature_celsius', ROUND(MAX(rd.value) FILTER (WHERE rd.sensor_name = 'Temperatura vazduha'), 2),
            'air-humidity_percent', ROUND(MAX(rd.value) FILTER (WHERE rd.sensor_name = 'Vlažnost vazduha'), 2),
            'precipitation_mm', ROUND(MAX(rd.value) FILTER (WHERE rd.sensor_name = 'Količina padavina'), 2),
            'dew-point_celsius', ROUND(MAX(rd.value) FILTER (WHERE rd.sensor_name = 'Tačka rose'), 2),
            'leaf-wetness_min', ROUND(MAX(rd.value) FILTER (WHERE rd.sensor_name = 'Vlažnost lista'), 2),
            'latitude_4326', mi.latitude,
            'longitude_4326', mi.longitude
        )) AS data
    FROM raw_data rd
    JOIN module_info mi ON rd.mac_address_lora_module = mi.mac_address
    GROUP BY mi.mac_address, mi.module_name, mi.latitude, mi.longitude, rd.date
    ORDER BY rd.date DESC
    '''


def get_rhmz_query(date_from, date_to, date_middle_1, date_middle_2):
    return '''
    -- RHMZ
    WITH params AS (
        SELECT 
            %s::varchar AS mac_address,
            %s::timestamp AS date_from,
            %s::timestamp AS date_to,
            %s::timestamp AS date_middle_1,
            %s::timestamp AS date_middle_2
    ),
    module_info AS (
        SELECT
            m.mac_address,
            m.name AS module_name,
            ROUND(ST_Y(ml.location::geometry)::decimal, 5) AS latitude,
            ROUND(ST_X(ml.location::geometry)::decimal, 5) AS longitude
        FROM lora_module m
        JOIN lora_module_location ml ON m.mac_address = ml.mac_address_lora_module
        JOIN params p ON m.mac_address = p.mac_address
    ),
    raw_data AS (
        SELECT
            lm.date,
            lm.value,
            st.name AS sensor_name,
            lm.mac_address_lora_module
        FROM lora_measurement lm
        JOIN lora_device_type_sensor_type ldt ON lm.id_lora_device_type_sensor_type = ldt.id
        JOIN lora_sensor_type st ON ldt.id_lora_sensor_type = st.id
        JOIN params p ON lm.mac_address_lora_module = p.mac_address
        WHERE
            lm.device_on IS TRUE
            AND lm.valid IS TRUE
            AND (
                -- Oba middle datuma postoje - dva perioda
                (p.date_middle_1 IS NOT NULL AND p.date_middle_2 IS NOT NULL AND (
                    (lm.date > p.date_from AND lm.date < p.date_middle_1) OR
                    (lm.date > p.date_middle_2 AND lm.date < p.date_to)
                ))
                OR
                -- Samo date_middle_1 postoji - period od date_from do date_middle_1
                (p.date_middle_1 IS NOT NULL AND p.date_middle_2 IS NULL AND (
                    lm.date > p.date_from AND lm.date < p.date_middle_1
                ))
                OR
                -- Samo date_middle_2 postoji - period od date_middle_2 do date_to
                (p.date_middle_1 IS NULL AND p.date_middle_2 IS NOT NULL AND (
                    lm.date > p.date_middle_2 AND lm.date < p.date_to
                ))
                OR
                -- Nijedan middle datum ne postoji - ceo period
                (p.date_middle_1 IS NULL AND p.date_middle_2 IS NULL AND (
                    lm.date > p.date_from AND lm.date < p.date_to
                ))
            )
            AND st.name IN (
                    'Brzina vetra',
                    'Količina padavina', 
                    'Smer vetra',
                    'Solarno zračenje',
                    'Tačka rose',
                    'Temperatura vazduha',
                    'Temperatura zemljišta',
                    'Udar vetra',
                    'Vazdušni pritisak',
                    'Vlažnost vazduha'
            )
    )
    SELECT
        (jsonb_build_object(
            'key', mi.mac_address,
            'name', mi.module_name,
            'timestamp', rd.date,
            'air-temperature_celsius', ROUND(MAX(rd.value) FILTER (WHERE rd.sensor_name = 'Temperatura vazduha'), 2),
            'air-humidity_percent', ROUND(MAX(rd.value) FILTER (WHERE rd.sensor_name = 'Vlažnost vazduha'), 2),
            'air-pressure_mbar', ROUND(MAX(rd.value) FILTER (WHERE rd.sensor_name = 'Vazdušni pritisak'), 2),
            'precipitation_mm', ROUND(MAX(rd.value) FILTER (WHERE rd.sensor_name = 'Količina padavina'), 2),
            'dew-point_celsius', ROUND(MAX(rd.value) FILTER (WHERE rd.sensor_name = 'Tačka rose'), 2),
            'wind-speed_m/s', ROUND(MAX(rd.value) FILTER (WHERE rd.sensor_name = 'Brzina vetra'), 2),
            'wind-direction_angle', ROUND(MAX(rd.value) FILTER (WHERE rd.sensor_name = 'Smer vetra'), 2),
            'wind-gust_m/s', ROUND(MAX(rd.value) FILTER (WHERE rd.sensor_name = 'Udar vetra'), 2),
            'solar-radiation_j/cm2', ROUND(MAX(rd.value) FILTER (WHERE rd.sensor_name = 'Solarno zračenje'), 2),
            'latitude_4326', mi.latitude,
            'longitude_4326', mi.longitude
        )) AS data
    FROM raw_data rd
    JOIN module_info mi ON rd.mac_address_lora_module = mi.mac_address
    GROUP BY mi.mac_address, mi.module_name, mi.latitude, mi.longitude, rd.date
    ORDER BY rd.date DESC
    '''


def send_data_in_batches(project_id, collection_id, write_key, data, batch_size=3000):
    """Sends data in batches"""
    total = len(data)
    for i in range(0, total, batch_size):
        batch = data[i:i+batch_size]
        print(f"   Sending batch {i+1}-{min(i+batch_size, total)} of {total} records...")
        send_data(project_id, collection_id, write_key, batch)

        
def fetch_module_data(mac_address, date_from_dt, date_to_dt, first_timestamp_dt, last_timestamp_dt):
    """Fetches data for given module"""

    if first_timestamp_dt == date_from_dt:
        date_middle_1 = None
    else:
        date_middle_1 = first_timestamp_dt

    if last_timestamp_dt == date_to_dt:
        date_middle_2 = None
    else:
        date_middle_2 = last_timestamp_dt

    if mac_address.startswith('PIS_'):
        QUERY = get_pis_query(date_from_dt, date_to_dt, date_middle_1, date_middle_2)
    else:
        QUERY = get_rhmz_query(date_from_dt, date_to_dt, date_middle_1, date_middle_2)

    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SET search_path TO agrosense, public;")
            cur.execute(QUERY, (mac_address, date_from_dt, date_to_dt, date_middle_1, date_middle_2))
            rows = cur.fetchall()
            return [row['data'] for row in rows]
    finally:
        conn.close()


def save_module_data_to_txt(mac_address, data):
    """Saves data in JSONL format"""
    if not data:
        return
    out_path = os.path.join('data', f'{mac_address}.txt')
    with open(out_path, 'w', encoding='utf-8') as f:
        for row in data:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def get_collections(project_id, read_key):
    """Fetches all project collections"""
    url = f"{BASE_URL}/projects/{project_id}/collections"
    headers = {"X-API-Key": read_key}

    try:    
        response = httpx.get(url, headers=headers)
    except httpx.ReadTimeout:
        print(f"❌ Timeout: Fetching collections failed")
        return []
    if response.status_code == 200:
        collections = response.json()
        print(f"✅ Fetched {len(collections)} collections")
        return collections
    else:
        print(f"❌ Error fetching collections: {response.text}")
        return []


def create_collection(project_id, master_key, station_type):
    """Creates new collection for project"""
    url = f"{BASE_URL}/projects/{project_id}/collections"
    headers = {"X-API-Key": master_key}

    if station_type == 'PIS':
        collection_body = {
            "name": "station_type_2",
            "description": "IoT data for Serbian pilot for station type 2",
            "tags": ["temperature", "humidity", "precipitation", "dew_point", "leaf-wetness"],
            "collection_schema": {
                "key": "PIS_BOGARAS",
                "name": "Bogaraš",
                "latitude_4326": 45.93855,
                "longitude_4326": 19.91273,
                "timestamp": "2023-12-31T23:00:00Z",
                "leaf-wetness_min": 22.5,
                "precipitation_mm": 0.0,
                "dew-point_celsius": 5.5,
                "air-humidity_percent": 100.0,
                "air-temperature_celsius": 5.61
            }
        }
    elif station_type == 'RHMZ':
        collection_body = {
            "name": "station_type_1",
            "description": "IoT data for Serbian pilot for station type 1",
            "tags": ["temperature", "precipitation", "humidity", "wind", "pressure", "solar_radiation", "dew_point"],
            "collection_schema": {
                "key": "MAC_adresa",
                "name": "Sremska Mitrovica",
                "timestamp": "2025-07-28T09:54:00Z",
                "air-temperature_celsius": 23.2,
                "precipitation_mm": 0.8,
                "air-humidity_percent": 75.2,
                "wind-speed_m/s": 2.6,
                "wind-direction_angle": 127.0,
                "wind-gust_m/s": 7.9,
                "dew-point_celsius": 4.6,
                "air-pressure_mbar": 997.3,
                "solar-radiation_j/cm2": 214.8,
                "latitude_4326": 45.22453,
                "longitude_4326": 19.58752
            }
        }
    else:
        raise ValueError("Invalid station type. Use 'PIS' or 'RHMZ'.")  

    try:    
        response = httpx.post(url, json=collection_body, headers=headers, timeout=15.0) 
    except httpx.ReadTimeout:
        print(f"❌ Timeout: Creating collection failed")
        return None
    if response.status_code == 200:
        print(f"✅ Collection created", response.json())
        return response.json().get("collection_id")
    else:
        print(f"❌ Error creating collection: {response.text}")
        return None


def delete_collection(project_id, master_key, collection_id):
    """Deletes collection"""
    url = f"{BASE_URL}/projects/{project_id}/collections/{collection_id}"
    headers = {"X-API-Key": master_key}

    try:
        response = httpx.delete(url, headers=headers, timeout=15.0)
        if response.status_code == 200:
            print(f"✅ Collection deleted", response.json())
        else:
            print(f"❌ Error deleting collection: {response.text}")
    except httpx.ReadTimeout:
        print(f"❌ Timeout: Deleting collection {collection_id} failed")
    except Exception as e:
        print(f"❌ Error deleting collection {collection_id}: {e}")
   

def delete_data(project_id, collection_id, master_key, key=None, timestamp_from=None, timestamp_to=None):
    """Delete data from collection based on criteria"""
    url = f"{BASE_URL}/projects/{project_id}/collections/{collection_id}/delete_data"
    headers = {"X-API-Key": master_key}

    delete_request = {}
    if key:
        delete_request["key"] = key
    if timestamp_from:
        delete_request["timestamp_from"] = timestamp_from
    if timestamp_to:
        delete_request["timestamp_to"] = timestamp_to

    response = requests.delete(url, json=delete_request, headers=headers)
    if response.status_code == 200:
        result = response.json()
        print(f"✅ Data deleted successfully: {result['message']}")
        return result
    else:
        print(f"Failed to delete data: {response.text}")
        return None


def fetch_lora_modules(station_prefix):
    """Fetches modules for given station type"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
    except psycopg2.OperationalError as e:
        # Catches connection issues (unreachable IP, wrong port, etc.)
        print(f"Error connecting to database: {e}")
        return []
    except psycopg2.Error as e:
        # Catches all other psycopg2 errors
        print(f"Database error: {e}")
        return []
    
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SET search_path TO agrosense, public;")
            cur.execute(
                """
                SELECT m.mac_address
                FROM lora_module m
                WHERE m.mac_address LIKE %s
                """,
                (f"%{station_prefix}%",)
            )
            results = cur.fetchall()
        return results
    finally:
        conn.close()


def send_data(project_id, collection_id, write_key, data):
    """Sends data to collection"""
    url = f"{BASE_URL}/projects/{project_id}/collections/{collection_id}/send_data"
    headers = {"X-API-Key": write_key}
    try:    
        response = httpx.post(url, json=data, headers=headers)
    except httpx.ReadTimeout:
        print(f"   ❌ Timeout: Sending data failed")
        return False
    if response.status_code == 200:
        print(f"   ✅ Sent {len(data)} records")
        return True
    else:
        print(f"   ❌ Error sending data: {response.text}")
        return False


def get_data(project_id, collection_id, read_key, filters=None, attributes=None, limit=None, order_by=None):
    """Fetches data from collection"""
    url = f"{BASE_URL}/projects/{project_id}/collections/{collection_id}/get_data"
    headers = {"X-API-Key": read_key}
    params = {}
    
    if order_by:
        params["order_by"] = order_by
    if attributes:
        params["attributes"] = attributes
    if limit:
        params["limit"] = limit
    if filters:
        params["filters"] = json.dumps(filters)

    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        # print(response.url)
        return response.json()  # Returns {'data': [...]}
    else:
        print(f"❌ Error fetching data: {response.status_code} - {response.text}")
        return {'data': []}  # Return empty response in same format


def setup_collections(state):
    """Sets up and checks collections"""
    os.makedirs('data', exist_ok=True)
    collections = get_collections(PROJECT_ID, READ_KEY)

    print(f"⏳ Please wait, checking existing collections...")

    # Mapping existing collections
    for c in collections:
        coll_name = c.get('collection_name')
        for station_type, config in STATION_CONFIG.items():
            if coll_name == config['collection_name']:
                config['collection_id'] = c['collection_id']
                state[f'{station_type.lower()}_collection_id'] = c['collection_id']
                data_on_server = get_statistics(PROJECT_ID, c['collection_id'], READ_KEY, "key", "distinct")
                state[f'fetched_data_{station_type.lower()}'] = data_on_server['key_statistics']
    
    # Creating missing collections
    for station_type, config in STATION_CONFIG.items():
        if not config['collection_id']:
            new_id = create_collection(PROJECT_ID, MASTER_KEY, station_type)
            config['collection_id'] = new_id
            state[f'{station_type.lower()}_collection_id'] = new_id
    
    print(f"\n✅ Collection IDs:")    
    for station_type, config in STATION_CONFIG.items():
        print(f"   {station_type}: {config['collection_id']}")
        if state[f'fetched_data_{station_type.lower()}'] is not None:
          print(f"   {station_type} modules #: {len(state[f'fetched_data_{station_type.lower()}'])}")
    print()
    
    return state


def fetch_and_display_data(state):
    """Fetches sample data from collections"""
    pis_id = STATION_CONFIG['PIS']['collection_id']
    rhmz_id = STATION_CONFIG['RHMZ']['collection_id']
    
    if not pis_id or not rhmz_id:
        print("\n⚠️ You must first run option 1 (Setup Collections)!")
        return state
    
    attributes = ['key', 'timestamp', 'air-temperature_celsius']
    
    print(f"\n📥 Fetching data from PIS collection ({pis_id})...")
    # Fetch latest data (limit=10, sorted by timestamp desc)
    response_pis = get_data(
        PROJECT_ID, 
        pis_id, 
        READ_KEY, 
        attributes=attributes,
        # attributes = ['key'],
        order_by='{"field": "timestamp", "order": "desc"}',
        limit=len(state['fetched_data_pis']),
    )
    
    print(f"📥 Fetching data from RHMZ collection ({rhmz_id})...")
    response_rhmz = get_data(
        PROJECT_ID, 
        rhmz_id, 
        READ_KEY, 
        attributes=attributes,
        order_by='{"field": "timestamp", "order": "desc"}',
        limit=len(state['fetched_data_rhmz'])
    )

    # Extract list from API response
    data_pis = response_pis.get('data', []) if isinstance(response_pis, dict) else []
    data_rhmz = response_rhmz.get('data', []) if isinstance(response_rhmz, dict) else []
    
    state['fetched_data_pis'] = data_pis
    state['fetched_data_rhmz'] = data_rhmz
    
    print(f"\n✅ Data fetched:")
    print(f"   PIS: {len(data_pis)} items")
    if data_pis:
        for first in data_pis:
            print(f"      Latest: {first.get('key')} @ {first.get('timestamp')} ({first.get('air-temperature_celsius')}°C)")
    print(f"   RHMZ: {len(data_rhmz)} items")
    if data_rhmz:
        for first in data_rhmz:
            print(f"      Latest: {first.get('key')} @ {first.get('timestamp')} ({first.get('air-temperature_celsius')}°C)")
    print()
    
    return state


def process_and_send_data(state):
    """Processes and sends data in batches"""
    # Check if collections are set up
    if not all(config['collection_id'] for config in STATION_CONFIG.values()):
        print("\n⚠️ You must first run option 1 (Setup Collections)!")
        return state

    for station_type, config in STATION_CONFIG.items():
        print(f"\n{'='*60}")
        print(f"🔄 Processing {station_type} stations")
        print(f"{'='*60}")
        
        modules = fetch_lora_modules(config['prefix'])
        print(f"   Found {len(modules)} modules")
        data_on_server = state.get(f'fetched_data_{station_type.lower()}', {})

        for row in modules:
            mac = row['mac_address']
            
            # Skip excluded modules
            if is_module_excluded(mac):
                print(f"   ⏭️  Skipping excluded module: {mac}")
                continue
            
            print(f"\n⚙️ Processing module: {mac}")

            # Proveri da li postoji station sa tim key-em
            station_data = next((item for item in data_on_server if item['key'] == mac), None)

            if station_data:
                print(f"🛰️ Station {mac} found on Nostradamus IoT server:")
                print(f"   Min timestamp: {station_data['min_timestamp']}")
                print(f"   Max timestamp: {station_data['max_timestamp']}")
                print(f"   Total records: {station_data['total_records']}")
            else:
                print(f"🛈 Station {mac} not found on Nostradamus IoT server, will fetch all data from {date_from} to {date_to}")

            # Convert date strings to datetime objects
            date_from_dt = datetime.fromisoformat(date_from.replace('Z', '+00:00')).replace(tzinfo=utc)
            date_to_dt = datetime.fromisoformat(date_to.replace('Z', '+00:00')).replace(tzinfo=utc)

            if station_data is not None:
                first_timestamp_dt = datetime.fromisoformat(station_data['min_timestamp'].replace('Z', '+00:00')).replace(tzinfo=utc)
                last_timestamp_dt = datetime.fromisoformat(station_data['max_timestamp'].replace('Z', '+00:00')).replace(tzinfo=utc)
            else :
                first_timestamp_dt = datetime.fromisoformat(date_from.replace('Z', '+00:00')).replace(tzinfo=utc)
                last_timestamp_dt = datetime.fromisoformat(date_to.replace('Z', '+00:00')).replace(tzinfo=utc)

            if (not station_data) or  ((first_timestamp_dt > date_from_dt) or (last_timestamp_dt < date_to_dt)) :
                # Fetch data from database
                data = fetch_module_data(mac, date_from_dt, date_to_dt, first_timestamp_dt, last_timestamp_dt)
                if not data:
                    print(f"   ⚠️  No data fetched from local database for {mac}")
                    continue
                
                # Send data
                collection_id = get_collection_id_for_mac(mac)
                send_data_in_batches(PROJECT_ID, collection_id, WRITE_KEY, data, batch_size=2000)
                print(f"   ✅ Data sent to collection {collection_id}")
            elif (first_timestamp_dt <= date_from_dt) or (last_timestamp_dt >= date_to_dt):
                print(f"   ⏭️  Skipping {mac} - data is already complete up to {date_to}")
                continue


    
    print(f"\n{'='*60}")
    print("🎉 Processing complete!")
    print(f"{'='*60}\n")
    
    return state


def get_first_timestamps_for_station(collection_id, station_type):
    """Fetches first timestamp for MAC address"""
      
    filters = [
        {
            "property_name": "key",
            "operator": "eq",
            "property_value": station_type
        }
    ]
    
    response = get_data(
        PROJECT_ID,
        collection_id,
        READ_KEY,
        attributes=['key', 'timestamp'],
        filters=filters,
        order_by='{"field": "timestamp", "order": "asc"}',
        limit=1
    )
    
    # Extract list from response
    first_timestamps = response.get('data', []) if isinstance(response, dict) else []
    
    if first_timestamps and len(first_timestamps) > 0:
        timestamp = first_timestamps[0]['timestamp']
        print(f"   ✅ {station_type}: {timestamp}")
        return timestamp
    else:
        print(f"   ⚠️  {station_type}: No data")
        return None
    

def get_last_timestamps_for_station(collection_id, station_type):
    """Fetches last timestamp for MAC address"""
    
    filters = [
        {
            "property_name": "key",
            "operator": "eq",
            "property_value": station_type
        }
    ]
    
    response = get_data(
        PROJECT_ID,
        collection_id,
        READ_KEY,
        attributes=['key', 'timestamp'],
        filters=filters,
        order_by='{"field": "timestamp", "order": "desc"}',
        limit=1
    )
    
    # Extract list from response
    latest_timestamps = response.get('data', []) if isinstance(response, dict) else []
    
    if latest_timestamps and len(latest_timestamps) > 0:
        timestamp = latest_timestamps[0]['timestamp']
        print(f"   ✅ {station_type}: {timestamp}")
        return timestamp
    else:
        print(f"   ⚠️  {station_type}: No data")
        return None


def get_latest_timestamps_per_key(state):
    """Fetches latest timestamps for each MAC address"""
    pis_id = STATION_CONFIG['PIS']['collection_id']
    rhmz_id = STATION_CONFIG['RHMZ']['collection_id']
    
    if not pis_id or not rhmz_id:
        print("\n⚠️ You must first run option 1 (Setup Collections)!")
        return state
    
    print(f"\n{'='*60}")
    print("🔍 Finding latest timestamps per MAC address")
    print(f"{'='*60}\n")
    
    for station_type, config in STATION_CONFIG.items():
        collection_id = config['collection_id']
        print(f"📡 {station_type} stations:")
        
        # Fetch all unique keys
        response = get_data(
            PROJECT_ID,
            collection_id,
            READ_KEY,
            attributes=['key']
        )
        
        # Extract data from API response
        all_keys_data = response.get('data', []) if isinstance(response, dict) else []
        
        # Extract unique MAC addresses
        unique_keys = list(set([item['key'] for item in all_keys_data if 'key' in item]))
        print(f"   Found {len(unique_keys)} unique MAC addresses\n")
        
        latest_timestamps = {}
        
        # For each MAC address, fetch latest timestamp
        for mac_key in sorted(unique_keys):
            # Filter for specific MAC address + sorting + limit 1
            filters = [
                {
                    "property_name": "key",
                    "operator": "eq",
                    "property_value": mac_key
                }
            ]
            
            response = get_data(
                PROJECT_ID,
                collection_id,
                READ_KEY,
                attributes=['key', 'timestamp'],
                filters=filters,
                order_by='{"field": "timestamp", "order": "desc"}',
                limit=1
            )
            
            # Extract list from response
            latest_data = response.get('data', []) if isinstance(response, dict) else []
            
            if latest_data and len(latest_data) > 0:
                timestamp = latest_data[0]['timestamp']
                latest_timestamps[mac_key] = timestamp
                print(f"   ✅ {mac_key}: {timestamp}")
            else:
                print(f"   ⚠️  {mac_key}: No data")
        
        state[f'{station_type.lower()}_latest_timestamps'] = latest_timestamps
        print()
    
    print(f"{'='*60}\n")
    return state


def delete_menu(state):
    """Menu for deleting collections"""
    while True:
        print("="*60)
        print("🤖 NOSTRADAMUS Data Processor - Interactive Menu")
        print("="*60)
        print("1. Delete PIS collection")
        print("2. Delete RHMZ collection")
        print("R. Return to main menu")
        print("="*60)
        
        choice = input("➤ Choose option: ").strip().upper()
        if choice.upper() == 'R':
            print("\n↩️  Returning to main menu...")
            break

        elif choice == '1':
            print(f"\n{'='*60}")
            print("📊 DELETING PIS COLLECTION")
            print(f"{'='*60}")
            if STATION_CONFIG['PIS']['collection_id'] is None:
                print("❌ PIS collection ID is not set. Cannot delete.")        
            else:
                delete_collection(PROJECT_ID, MASTER_KEY, STATION_CONFIG['PIS']['collection_id'])
                state['pis_collection_id'] = None

        elif choice == '2':
            print(f"\n{'='*60}")
            print("📊 DELETING RHMZ COLLECTION")
            print(f"{'='*60}")
            if STATION_CONFIG['RHMZ']['collection_id'] is None:
                print("❌ RHMZ collection ID is not set. Cannot delete.")
            else:
                delete_collection(PROJECT_ID, MASTER_KEY, STATION_CONFIG['RHMZ']['collection_id'])
                state['rhmz_collection_id'] = None

        else:
            print(f"\n❌ Unknown option: {choice}\n")

        setup_collections(state)

def interactive_menu():
    """Interactive menu"""
    state = {
        'pis_collection_id': None,
        'rhmz_collection_id': None,
        'fetched_data_pis': None,
        'fetched_data_rhmz': None,
    }

    menu_actions = {
        '1': setup_collections,
        '2': fetch_and_display_data,
        '3': process_and_send_data,
        '5': get_latest_timestamps_per_key,
        '6': delete_menu,
    }

    while True:
        print("="*60)
        print("🤖 NOSTRADAMUS Data Processor - Interactive Menu")
        print("="*60)
        print("1. Setup/Check PIS & RHMZ collections")
        print("2. Fetch sample data (requires step 1)")
        print("3. Process and send all data (requires step 1)")
        print("4. Display current state (requires step 1)")
        print("5. Find latest timestamps per MAC address (requires step 1)")
        print("6. Delete selected collections (requires step 1)")
        print("Q. Exit")
        print("="*60)
        
        choice = input("➤ Choose option: ").strip().upper()

        if choice.upper() == 'Q':
            print("\n👋 Goodbye!")
            break
            
        elif choice == '4':
            print(f"\n{'='*60}")
            print("📊 CURRENT STATE")
            print(f"{'='*60}")
            
            # Display collections
            print("\n📁 Collections:")
            for station_type, config in STATION_CONFIG.items():
                status = "✅" if config['collection_id'] else "❌"
                print(f"   {status} {station_type}: {config['collection_id']}")
            
            # Display fetched data
            print("\n📥 Fetched data:")
            for key, value in state.items():
                if 'fetched_data' in key and value:
                    print(f"   ✅ {key}: {len(value)} items")
                    if value:
                        sample = value[0]
                        print(f"      Sample: {sample.get('key', 'N/A')} | Total records: {sample.get('total_records', 'N/A')}")
                elif 'fetched_data' in key:
                    print(f"   ⚪ {key}: No data")
            
            print(f"{'='*60}\n")
            
        elif choice in menu_actions:

            state = menu_actions[choice](state)
            
        else:
            print(f"\n❌ Unknown option: {choice}\n")


def get_statistics(project_id, collection_id, read_key, attribute, stat=None, filters=None, order=None, interval=None):
    """Get statistics for attribute"""
    url = f"{BASE_URL}/projects/{project_id}/collections/{collection_id}/statistics"
    headers = {"X-API-Key": read_key}


    params = {
        "attribute": attribute,
        "stat": stat,
        "interval": interval,
        "order": order
    }

    if filters:
        params["filters"] = json.dumps(filters)

    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        stats = response.json()
        # print(f"✅ {stat} for {attribute}: {stats}")
        return stats
    else:
        print(f"Failed to get statistics: {response.text}")
        return {}


def main():
    """Main function - launches menu"""
    print("\n" + "="*60)
    print("🚀 NOSTRADAMUS IoT Data Processor")
    print("="*60)
    print(f"📅 Period: {date_from} to {date_to}")
    print(f"🗄️  Database: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}")
    print(f"🌐 API: {BASE_URL}")
    print("="*60 + "\n")
    
    interactive_menu()


if __name__ == "__main__":
    main()
