#!/usr/bin/env python3
"""
Live data processor - automatically fetches and sends data from last hour
without user interaction.
Replace CHANGE_ME!!! 
"""

import os
import psycopg2
import json
import httpx
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta
import requests
import pytz

utc = pytz.UTC

# Configuration
BASE_URL = "https://nostradamus-ioto.issel.ee.auth.gr/api/v1"
PROJECT_ID = "6e2bcf44-f12a-4acd-853a-1468752785a8"

# API Keys
MASTER_KEY = 'CHANGE_ME'
WRITE_KEY = 'CHANGE_ME'
READ_KEY = 'd1091a7ac0fea235a022005637ed1f23dd7de7bf50ddf82d69bf68e724ecb65a'

# Station Configuration
STATION_CONFIG = {
    'RHMZ': {
        'prefix': '_RHMZ',
        'collection_name': 'station_type_1',
        'collection_id': None,
        'excluded_modules': []
    },
    'PIS': {
        'prefix': 'PIS_',
        'collection_name': 'station_type_2',
        'collection_id': None,
        'excluded_modules': []
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


def get_pis_query():
    """Query for PIS data - fetches data after last_timestamp"""
    return '''
    WITH params AS (
        SELECT
            %s::varchar AS mac_address,
            %s::timestamp AS last_timestamp
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
            AND lm.date > p.last_timestamp
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
    ORDER BY rd.date ASC
    '''


def get_rhmz_query():
    """Query for RHMZ data - fetches data after last_timestamp"""
    return '''
    WITH params AS (
        SELECT
            %s::varchar AS mac_address,
            %s::timestamp AS last_timestamp
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
            AND lm.date > p.last_timestamp
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
    ORDER BY rd.date ASC
    '''


def fetch_module_data(mac_address, last_timestamp):
    """Fetches data for given module after last_timestamp"""

    if mac_address.startswith('PIS_'):
        QUERY = get_pis_query()
    else:
        QUERY = get_rhmz_query()

    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SET search_path TO agrosense, public;")
            cur.execute(QUERY, (mac_address, last_timestamp))
            rows = cur.fetchall()
            return [row['data'] for row in rows]
    finally:
        conn.close()


def get_collections(project_id, read_key):
    """Fetches all project collections"""
    url = f"{BASE_URL}/projects/{project_id}/collections"
    headers = {"X-API-Key": read_key}

    try:
        response = httpx.get(url, headers=headers, timeout=15.0)
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
        print(f"✅ Collection created: {response.json()}")
        return response.json().get("collection_id")
    else:
        print(f"❌ Error creating collection: {response.text}")
        return None


def fetch_lora_modules(station_prefix):
    """Fetches modules for given station type"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
    except psycopg2.OperationalError as e:
        print(f"❌ Error connecting to database: {e}")
        return []
    except psycopg2.Error as e:
        print(f"❌ Database error: {e}")
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
        response = httpx.post(url, json=data, headers=headers, timeout=30.0)
    except httpx.ReadTimeout:
        print(f"   ❌ Timeout: Sending data failed")
        return False
    if response.status_code == 200:
        print(f"   ✅ Sent {len(data)} records")
        return True
    else:
        print(f"   ❌ Error sending data: {response.text}")
        return False


def send_data_in_batches(project_id, collection_id, write_key, data, batch_size=2000):
    """Sends data in batches"""
    total = len(data)
    for i in range(0, total, batch_size):
        batch = data[i:i+batch_size]
        print(f"   Sending batch {i+1}-{min(i+batch_size, total)} of {total} records...")
        send_data(project_id, collection_id, write_key, batch)


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

    response = requests.get(url, headers=headers, params=params, timeout=30.0)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"❌ Error fetching data: {response.status_code} - {response.text}")
        return {'data': []}


def get_last_timestamp_for_module(collection_id, mac_address):
    """Fetches latest timestamp for specific module from server"""
    filters = [
        {
            "property_name": "key",
            "operator": "eq",
            "property_value": mac_address
        }
    ]

    response = get_data(
        PROJECT_ID,
        collection_id,
        READ_KEY,
        attributes=['timestamp'],
        filters=filters,
        order_by='{"field": "timestamp", "order": "desc"}',
        limit=1
    )

    latest_data = response.get('data', []) if isinstance(response, dict) else []

    if latest_data and len(latest_data) > 0:
        timestamp_str = latest_data[0]['timestamp']
        return datetime.fromisoformat(timestamp_str.replace('Z', '+00:00')).replace(tzinfo=utc)
    else:
        # If no data exists, return timestamp from 1 hour ago
        return datetime.now(utc) - timedelta(hours=1)


def setup_collections():
    """Sets up and checks collections"""
    print("⏳ Checking collections...")
    collections = get_collections(PROJECT_ID, READ_KEY)

    # Mapping existing collections
    for c in collections:
        coll_name = c.get('collection_name')
        for station_type, config in STATION_CONFIG.items():
            if coll_name == config['collection_name']:
                config['collection_id'] = c['collection_id']

    # Creating missing collections
    for station_type, config in STATION_CONFIG.items():
        if not config['collection_id']:
            print(f"Creating {station_type} collection...")
            new_id = create_collection(PROJECT_ID, MASTER_KEY, station_type)
            config['collection_id'] = new_id

    print(f"\n✅ Collection IDs:")
    for station_type, config in STATION_CONFIG.items():
        print(f"   {station_type}: {config['collection_id']}")
    print()


def process_and_send_live_data():
    """Processes and sends live data from last hour"""

    # Check if collections are set up
    if not all(config['collection_id'] for config in STATION_CONFIG.values()):
        print("\n⚠️ Collections not set up properly!")
        return

    now = datetime.now(utc)
    print(f"⏰ Processing data up to: {now.isoformat()}\n")

    total_records_sent = 0

    for station_type, config in STATION_CONFIG.items():
        print(f"\n{'='*60}")
        print(f"🔄 Processing {station_type} stations")
        print(f"{'='*60}")

        modules = fetch_lora_modules(config['prefix'])
        print(f"   Found {len(modules)} modules")

        for row in modules:
            mac = row['mac_address']

            # Skip excluded modules
            if is_module_excluded(mac):
                print(f"   ⏭️  Skipping excluded module: {mac}")
                continue

            print(f"\n⚙️  Processing module: {mac}")

            # Get last timestamp from server
            collection_id = get_collection_id_for_mac(mac)
            last_timestamp_server = get_last_timestamp_for_module(collection_id, mac)
            print(f"   Last timestamp on server: {last_timestamp_server.isoformat()}")

            # Use max of: last timestamp on server OR 1 hour ago
            # This ensures we only fetch last hour of data, but don't create duplicates
            one_hour_ago = now - timedelta(hours=1)
            last_timestamp = max(last_timestamp_server, one_hour_ago)

            if last_timestamp > last_timestamp_server:
                print(f"   Using 1 hour ago limit: {last_timestamp.isoformat()}")

            # Fetch new data from local database
            data = fetch_module_data(mac, last_timestamp)

            if not data:
                print(f"   ℹ️  No new data for {mac}")
                continue

            print(f"   📊 Found {len(data)} new records")

            # Send data
            send_data_in_batches(PROJECT_ID, collection_id, WRITE_KEY, data, batch_size=2000)
            total_records_sent += len(data)
            print(f"   ✅ Data sent to collection {collection_id}")

    print(f"\n{'='*60}")
    print(f"🎉 Processing complete! Total records sent: {total_records_sent}")
    print(f"{'='*60}\n")


def main():
    """Main function - runs automatically without user interaction"""
    print("\n" + "="*60)
    print("🚀 NOSTRADAMUS Live Data Processor")
    print("="*60)
    print(f"🗄️  Database: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}")
    print(f"🌐 API: {BASE_URL}")
    print("="*60 + "\n")

    try:
        # Setup collections
        setup_collections()

        # Process and send live data
        process_and_send_live_data()

        print("✅ Script completed successfully!")

    except Exception as e:
        print(f"\n❌ Error occurred: {e}")
        import traceback
        traceback.print_exc()
        exit(1)


if __name__ == "__main__":
    main()
