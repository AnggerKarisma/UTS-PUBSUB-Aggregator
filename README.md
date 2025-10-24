Layanan aggregator event berbasis FastAPI dengan fitur deduplication dan persistence menggunakan SQLite.

### Docker 
```bash
# Build
docker build -t uts-aggregator .

# Run
docker run -p 8080:8080 uts-aggregator
```

### Local Development
```bash
# Install dependencies
pip install -r requirements.txt

## API Endpoints

### 1. POST /publish
Kirim event (single atau batch).

**Single Event:**
```bash
curl -X POST http://localhost:8080/publish \
  -H "Content-Type: application/json" \
  -d '{
    "topic": "user.created",
    "event_id": "evt-001",
    "timestamp": "2025-10-24T10:30:00Z",
    "source": "user-service",
    "payload": {"user_id": "123"}
  }'
```

**Batch Events:**
```bash
curl -X POST http://localhost:8080/publish \
  -H "Content-Type: application/json" \
  -d '[
    {"topic": "order.created", "event_id": "order-001", "timestamp": "2025-10-24T10:30:00Z", "source": "order-service", "payload": {}},
    {"topic": "order.created", "event_id": "order-002", "timestamp": "2025-10-24T10:31:00Z", "source": "order-service", "payload": {}}
  ]'
```

### 2. GET /events
List events (opsional filter by topic).

```bash
# Semua events
curl http://localhost:8080/events

# Filter by topic
curl http://localhost:8080/events?topic=user.created
```

### 3. GET /stats
Monitoring metrics.

```bash
curl http://localhost:8080/stats
```

Response:
```json
{
  "received": 1000,
  "unique_processed": 850,
  "duplicate_dropped": 150,
  "topics": ["user.created", "order.created"],
  "uptime_seconds": 3600.5
}
```

##  Fitur Utama

- **Idempotent**: Event dengan `(topic, event_id)` sama hanya diproses sekali
- **Persistent**: Dedup store di SQLite, tahan restart
- **Batch Support**: Kirim banyak events sekaligus
- **High Performance**: Tested 5000+ events

##  Testing

```bash
# Run all tests
pytest -v

# Run specific tests
pytest tests/test_publish_and_dedup.py -v
pytest tests/test_stress_5000_events.py -v
```

**Test Coverage:**
- ✅ Deduplication logic (6 tests)
- ✅ Schema validation (7 tests)
- ✅ Stats consistency (6 tests)
- ✅ Persistence after restart (2 tests)
- ✅ Stress tests 5000+ events (5 tests)

##  Deduplication Logic

Event dianggap **duplicate** jika pasangan `(topic, event_id)` sudah pernah diproses.

**Contoh:**
```
Event 1: topic="user.created", event_id="evt-001" → ✅ Processed
Event 2: topic="user.created", event_id="evt-001" → ❌ Dropped (duplicate)
Event 3: topic="order.created", event_id="evt-001" → ✅ Processed (topic berbeda)
```

##  Arsitektur

```
Publisher → POST /publish → asyncio.Queue → ConsumerWorker → DedupStore (SQLite)
```

- **asyncio.Queue**: In-memory queue untuk pipeline event
- **ConsumerWorker**: Background worker proses event secara async
- **DedupStore**: SQLite dengan PRIMARY KEY (topic, event_id)

##  Asumsi & Limitasi

**Asumsi:**
- Single instance (no distributed setup)
- Local SQLite storage
- At-least-once delivery semantic

**Ordering:**
- Tidak menjamin total ordering antar events
- Deduplication tidak bergantung pada ordering
- Focus pada "apakah event sudah diproses", bukan urutan

**Performance:**
- Throughput: ~2000-3000 events/sec
- Tested hingga 5000 events dengan 50% duplicate rate

##  Health Check

```bash
curl http://localhost:8080/stats
```

Container health check otomatis berjalan setiap 30 detik.

video Demo : https://youtu.be/NZUQqY7988o