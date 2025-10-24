# Pub-Sub Log Aggregator (UTS)


## Build


docker build -t uts-aggregator .


## Run


docker run -p 8080:8080 uts-aggregator


## Endpoints


- POST /publish -> JSON single event or array of events
- GET /events?topic=... -> list processed events for a topic
- GET /stats -> metrics


## Notes
- Deduplication berbasis (topic, event_id) disimpan di SQLite lokal.
- Consumer berjalan secara asinkron di background.
- Tidak menjamin total ordering global.