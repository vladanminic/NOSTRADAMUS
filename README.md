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
├── main.py          # Interactive application for historical data
├── live.py          # Automated script for live data sync
├── readme.md        # Documentation
```

## 🚀 Execution

### Main Application (Interactive Mode)

```bash
python main.py
```

The application launches an interactive menu for historical data processing:

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

### Live Data Processor (Automated Mode)

```bash
python live.py
```

The live processor runs automatically without user interaction and:
- Detects existing collections or creates new ones
- Checks the latest timestamp for each module on the server
- Fetches only new data from the local database
- Sends data to appropriate collections
- Avoids sending duplicate records
- Exits automatically after completion

**Use cases:**
- **Scheduled execution** (cron/Task Scheduler) for continuous data sync
- **Real-time monitoring** - fetch and send recent data
- **Automated pipelines** - no manual intervention required

**Key differences from main.py:**

| Feature | main.py | live.py |
|---------|---------|---------|
| Mode | Interactive menu | Automated execution |
| Time period | User-defined date range | Automatic (from last server timestamp) |
| User input | Required | None |
| Use case | Historical data bulk import | Live/recent data sync |
| Duplicate handling | Complex date range logic | Simple timestamp comparison |
| Exit behavior | Manual exit (Q) | Automatic after completion |

## 📊 Operation Sequence (main.py)

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

### main.py - Historical Data Queries
Complex queries with custom date ranges and middle date handling for flexible time period selection:
- Supports multiple time periods with `date_middle_1` and `date_middle_2`
- Allows excluding specific date ranges
- Used for bulk historical data import

### live.py - Incremental Data Queries
Simplified queries that fetch only new records:
- Uses single `last_timestamp` parameter
- Fetches data where `date > last_timestamp`
- Optimized for real-time synchronization

**PIS Sensors:**
- Air temperature
- Air humidity
- Precipitation
- Dew point
- Leaf wetness

**RHMZ Sensors:**
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

| Function | File | Description |
|----------|------|-------------|
| `setup_collections()` | Both | Initialize collections |
| `fetch_lora_modules()` | Both | Retrieve list of LoRa modules |
| `fetch_module_data()` | Both | Fetch module data from database |
| `send_data_in_batches()` | Both | Send data in batches |
| `get_data()` | Both | Query data from API |
| `get_statistics()` | main.py | Retrieve statistics |
| `delete_data()` | main.py | Delete data records |
| `get_last_timestamp_for_module()` | live.py | Get latest timestamp for a module |
| `process_and_send_live_data()` | live.py | Automated live data processing |

## ⚠️ Important Notes

1. **Credentials** - API keys and passwords should be stored in `.env` file for production
2. **Time zones** - Uses UTC for all timestamps
3. **Batch size** - Set to 2000 records for optimal performance
4. **Excluded modules** - Specific modules can be excluded from processing

## ⏰ Automated Scheduling (live.py)

### Linux/macOS (cron)

Edit crontab:
```bash
crontab -e
```

Run every hour:
```cron
0 * * * * cd /path/to/nostradamus/python && python3 live.py >> logs/live.log 2>&1
```

Run every 15 minutes:
```cron
*/15 * * * * cd /path/to/nostradamus/python && python3 live.py >> logs/live.log 2>&1
```

### Windows (Task Scheduler)

Create a new task:
1. Open Task Scheduler
2. Create Basic Task → Name it "Nostradamus Live Sync"
3. Trigger: Daily at 00:00, repeat every 1 hour
4. Action: Start a program
   - Program: `python`
   - Arguments: `live.py`
   - Start in: `C:\path\to\nostradamus\python`
5. Finish

Or use PowerShell:
```powershell
$action = New-ScheduledTaskAction -Execute "python" -Argument "live.py" -WorkingDirectory "C:\path\to\nostradamus\python"
$trigger = New-ScheduledTaskTrigger -Daily -At 00:00 -RepetitionInterval (New-TimeSpan -Hours 1)
Register-ScheduledTask -TaskName "NostradamusLiveSync" -Action $action -Trigger $trigger
```

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

**Version**: 1.1
**Last Updated**: 2026-02-03
**Status**: Active ✅
**Author**: Vladan Minić - BioSense Institute

## 📝 Changelog

### Version 1.1 (2026-02-03)
- ✨ Added `live.py` - automated script for live data synchronization
- 📖 Enhanced documentation with scheduling examples
- 🔄 Simplified queries for incremental data sync
- ⚡ Optimized for real-time monitoring

### Version 1.0 (2025-12-30)
- 🎉 Initial release with `main.py`
- 📊 Interactive menu for historical data processing
- 🗄️ PostgreSQL database integration
- 🌐 Nostradamus IoT API integration
