# Swedish Car Price Scraper with Kafka

A data engineering project to scrape, stream, and analyze used car prices in Sweden.

## 🚀 Quick Start

### Prerequisites
- Docker & Docker Compose installed
- Python 3.8+ installed
- Git (optional)

### Step 1: Start Kafka Infrastructure

```bash
# Start Zookeeper and Kafka
docker-compose up -d

# Verify containers are running
docker ps

# You should see: zookeeper, kafka, and kafka-ui running
```

**Access Kafka UI**: Open http://localhost:8080 in your browser to monitor topics and messages

### Step 2: Setup Python Environment

```bash
# Create virtual environment
python -m venv venv

# Activate it
# On Linux/Mac:
source venv/bin/activate
# On Windows:
venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### Step 3: Create Kafka Topics

```bash
# Connect to Kafka container
docker exec -it kafka bash

# Create topics
kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic raw-car-listings \
  --partitions 3 \
  --replication-factor 1

kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic processed-listings \
  --partitions 3 \
  --replication-factor 1

kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic price-changes \
  --partitions 3 \
  --replication-factor 1

# List topics to verify
kafka-topics --list --bootstrap-server localhost:9092

# Exit container
exit
```

### Step 4: Run the Scraper

```bash
python car_scraper.py
```

### Step 5: Monitor Messages

**Option A: Using Kafka UI**
- Go to http://localhost:8080
- Click on "Topics" → "raw-car-listings"
- View messages in real-time

**Option B: Using Console Consumer**
```bash
docker exec -it kafka bash

kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic raw-car-listings \
  --from-beginning \
  --property print.key=true \
  --property key.separator=" : "
```

## 📊 Project Architecture

```
Web Scraper (Python)
    ↓
Kafka Producer → raw-car-listings topic
    ↓
[Future: Kafka Streams/Spark Processing]
    ↓
[Future: HDFS + Oracle DB]
    ↓
[Future: Airflow Orchestration]
```

## ⚠️ Important Notes

### About the Blocket API
- Uses the **blocket_api** Python package (unofficial but maintained)
- Respects rate limits automatically
- Provides structured data - no HTML parsing needed!
- Supports advanced search filters (location, price, year, mileage, etc.)

### Search Examples

The scraper includes three pre-configured searches:
1. **All Stockholm Cars** - Latest listings from Stockholm
2. **Potential Classics** - Cars 15-30 years old (the appreciation window!)
3. **Budget Cars** - Under 50,000 SEK

You can customize these or add your own in `car_scraper.py`.

### Testing the API

You can test searches directly:

```python
from blocket_api import BlocketAPI, Location, CarSortOrder

api = BlocketAPI()

# Search for Volvo cars in Stockholm
results = api.search_car(
    query="Volvo",
    locations=[Location.STOCKHOLM],
    year_from=2000,
    year_to=2010,
    price_to=100000,
    sort_order=CarSortOrder.PRICE_ASC
)

for ad in results[:5]:  # First 5 results
    print(f"{ad.subject} - {ad.price} SEK")
```

## 🛠️ Troubleshooting

### Kafka won't start
```bash
# Check logs
docker-compose logs kafka

# Restart services
docker-compose down
docker-compose up -d
```

### Connection refused to Kafka
- Wait 30-60 seconds after starting - Kafka takes time to initialize
- Check if port 9092 is available: `netstat -an | grep 9092`

### Scraper errors
- The blocket_api package handles most edge cases
- If you get connection errors, check your internet connection
- API rate limits are handled automatically with retries

## 🎯 Next Steps

1. **Enhance the scraper**: Adapt to real Blocket structure, add more sites (Bytbil, Kvdbil)
2. **Add Kafka consumer**: Process and transform the raw data
3. **Setup HDFS**: Store historical data
4. **Configure Oracle DB**: Design schema for analytics
5. **Build Airflow DAGs**: Orchestrate the pipeline
6. **Add analytics**: Identify price inflection points for classic cars

## 📚 Resources

- [Kafka Python Documentation](https://kafka-python.readthedocs.io/)
- [Blocket.se](https://www.blocket.se/annonser/hela_sverige/fordon/bilar)
- [BeautifulSoup Documentation](https://www.crummy.com/software/BeautifulSoup/bs4/doc/)

## 🔄 Stopping the Services

```bash
# Stop but keep data
docker-compose stop

# Stop and remove everything (including data)
docker-compose down -v
```

---

**Ready to analyze when classic cars start appreciating! 📈🚗**