# 🚀 NOSTRADAMUS IoT Data Processor

Data processing and transmission pipeline for IoT sensor data from LoRa stations (PIS and RHMZ) to the Nostradamus IoT server.

## 📋 Overview

The application provides:
- **Collection management** on Nostradamus IoT server
- **Data retrieval** from PostgreSQL database (AgroSense)
- **Data transmission** to Nostradamus IoT API
- **Data querying** with filtering and sorting capabilities
- **Data analysis** by MAC addresses and time periods

## 🛠️ Technologies

- **Python 3.x**
- **PostgreSQL** - local database with sensor data
- **Nostradamus IoT API** - cloud platform for data storage
- Libraries: `psycopg2`, `httpx`, `requests`, `pytz`

## ⚙️ Configuration

### Database Connection
```python
DB_CONFIG = {
    'dbname': 'CHANGE_ME',
    'user': 'CHANGE_ME',
    'password': 'CHANGE_ME',
    'host': 'CHANGE_ME',
    'port': 5432
}
```

### API Keys
```python
BASE_URL = "https://nostradamus-ioto.issel.ee.auth.gr/api/v1"
PROJECT_ID = "6e2bcf44-f12a-4acd-853a-1468752785a8"

MASTER_KEY = 'CHANGE_ME'  # Collection management
WRITE_KEY = 'CHANGE_ME'   # Data transmission
READ_KEY = 'CHANGE_ME'    # Data retrieval
```

### Supported Stations
- **PIS** - Precision Agriculture Information Stations
  - Air temperature, Humidity, Precipitation, Dew point, Leaf wetness
- **RHMZ** - Hydrometeorological Institute Stations
  - Temperature, Humidity, Pressure, Precipitation, Wind speed/direction, Solar radiation

## 📁 Project Structure

```
nostradamus/python/
├── main.py          # Main application
├── readme.md        # Documentation
```

## 🚀 Execution

```bash
python main.py
```

The application launches an interactive menu:

```
============================================================
🤖 NOSTRADAMUS Data Processor - Interactive Menu
============================================================
1. Setup/Check PIS & RHMZ collections
2. Fetch sample data (requires step 1)
3. Process and send all data (requires step 1)
4. Display current state (requires step 1)
5. Find latest timestamps per MAC address (requires step 1)
6. Delete selected collections (requires step 1)
Q. Exit
============================================================
```

## 📊 Operation Sequence

### 1️⃣ Setup Collections
```
Option: 1
```
- Verifies existing collections
- Retrieves station list from server
- Creates missing collections

### 2️⃣ Fetch Sample Data
```
Option: 2
```
- Retrieves last 10 records from both collections
- Displays latest temperature readings

### 3️⃣ Process & Send Data
```
Option: 3
```
- Iterates through all LoRa modules (PIS and RHMZ)
- Fetches data from PostgreSQL
- Groups by stations
- Sends in batches (2000 records)
- Skips already complete periods

### 4️⃣ Find Latest Timestamps
```
Option: 5
```
- Finds latest timestamp for each MAC address
- Displays data retrieval status per station

## 📝 SQL Queries

### PIS Query
Retrieves aggregated sensor values for PIS stations with custom time periods:
- Air temperature
- Air humidity
- Precipitation
- Dew point
- Leaf wetness

### RHMZ Query
Retrieves complete sensor set for RHMZ stations:
- Air temperature and humidity
- Pressure
- Precipitation
- Wind (speed, direction, gust)
- Solar radiation

## 🔗 API Examples

### Send Data
```python
send_data(
    PROJECT_ID, 
    collection_id, 
    WRITE_KEY, 
    [
        {
            'key': 'PIS_BOGARAS',
            'name': 'Bogaraš',
            'timestamp': '2023-12-31T23:00:00Z',
            'air-temperature_celsius': 5.61,
            'air-humidity_percent': 100.0,
            ...
        }
    ]
)
```

### Query Data with Filters
```python
get_data(
    PROJECT_ID,
    collection_id,
    READ_KEY,
    attributes=['key', 'timestamp', 'air-temperature_celsius'],
    filters=[{
        'property_name': 'key',
        'operator': 'eq',
        'property_value': 'PIS_BOGARAS'
    }],
    order_by='{"field": "timestamp", "order": "desc"}',
    limit=10
)
```

### Statistics
```python
get_statistics(
    PROJECT_ID,
    collection_id,
    READ_KEY,
    attribute='air-temperature_celsius',
    stat='distinct'  # or 'min', 'max', 'avg', 'count'
)
```

## 📥 Data Format - JSONL

Data is stored in the `data/` folder in JSONL format (one JSON object per line):

```json
{"key":"PIS_COKA","name":"Čoka","timestamp":"2022-01-01T00:00:00Z","air-temperature_celsius":1.23,"air-humidity_percent":85.5,"precipitation_mm":0.0,"dew-point_celsius":-3.21,"leaf-wetness_min":22.5,"latitude_4326":45.93855,"longitude_4326":19.91273}
{"key":"PIS_COKA","name":"Čoka","timestamp":"2022-01-01T01:00:00Z","air-temperature_celsius":1.45,"air-humidity_percent":84.2,"precipitation_mm":0.0,"dew-point_celsius":-3.05,"leaf-wetness_min":20.1,"latitude_4326":45.93855,"longitude_4326":19.91273}
```

## 🔍 Key Functions

| Function | Description |
|----------|-------------|
| `setup_collections()` | Initialize collections |
| `fetch_lora_modules()` | Retrieve list of LoRa modules |
| `fetch_module_data()` | Fetch module data from database |
| `send_data_in_batches()` | Send data in batches |
| `get_data()` | Query data from API |
| `get_statistics()` | Retrieve statistics |
| `delete_data()` | Delete data records |

## ⚠️ Important Notes

1. **Credentials** - API keys and passwords should be stored in `.env` file for production
2. **Time zones** - Uses UTC for all timestamps
3. **Batch size** - Set to 2000 records for optimal performance
4. **Excluded modules** - Specific modules can be excluded from processing

## 🐛 Troubleshooting

### Error: Database connection failed
```
Verify IP address (YOUR DB IP), port (5432) and credentials
```

### Error: Timeout during data transmission
```
Reduce batch_size or verify connection to Nostradamus server
```

### No data for module
```
Module may be offline or has no sensor values in the specified period
```

## 📚 Additional Resources

- **API Documentation**: https://nostradamus-ioto.issel.ee.auth.gr/api/docs
- **Jupyter Examples**: https://colab.research.google.com/drive/1Uu12nIu1LhkTnb5Y-Sq1ZqeZkn3mjWNE

## 📧 Support

For questions or issues, contact the development team.

---

**Version**: 1.0  
**Last Updated**: 2025-12-30  
**Status**: Active ✅  
**Author**: Vladan Minić - BioSense Institute
