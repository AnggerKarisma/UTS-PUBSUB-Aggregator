import asyncio
from contextlib import asynccontextmanager
import logging
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import List
from .model import Event
from .dedup_store import DedupStore
from .worker import ConsumerWorker
from .utils import uptime_seconds
import os


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('aggregator')
log_level = os.environ.get('LOG_LEVEL', 'INFO')
logging.basicConfig(
    level=getattr(logging, log_level),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Startup : memulai worker...")
    db_path = os.environ.get('DEDUP_DB_PATH', 'dedup.db')
    app.state.dedup = DedupStore(db_path)
    app.state.queue = asyncio.Queue()
    app.state.processed_events = []
    app.state.counters = {'received': 0}
    app.state.worker = ConsumerWorker(app.state.queue, app.state.dedup, app.state.processed_events)
    app.state._consumer_task = asyncio.create_task(app.state.worker.start())
    try:
        yield
    finally:
        print("Shutdown : menghentikan worker...")
        app.state.worker.stop()
        if hasattr(app.state, '_consumer_task'):
            app.state._consumer_task.cancel()

def create_app() -> FastAPI:
    app = FastAPI(title='UTS PubSub Aggregator', lifespan=lifespan)
    app.add_middleware(CORSMiddleware, allow_origins=['*'], allow_methods=['*'], allow_headers=['*'])

    @app.post('/publish')
    async def publish(payload: Event | list[Event]):
        try:
            if isinstance(payload, Event):
                events = [payload.model_dump()]
            elif isinstance(payload, list):
                events = [p.model_dump() for p in payload]

            for e in events:
                await app.state.queue.put(e)
                app.state.counters['received'] += 1

            return {'accepted': len(events)}
        except Exception as e:
            logger.error(f"Error publishing events: {e}")
            raise HTTPException(status_code=500, detail=str(e))


    @app.get('/events')
    async def get_events(topic: str = Query(None)):
        if topic:
            rows = app.state.dedup.list_events_for_topic(topic)
            return [{'event_id': r[0], 'processed_at': r[1]} for r in rows]
        return app.state.processed_events


    @app.get('/stats')
    async def stats():
        unique = app.state.dedup.count_processed()
        return {
                'received': app.state.counters['received'],
                'unique_processed': unique,
                'duplicate_dropped': app.state.counters['received'] - unique,
                'topics': app.state.dedup.list_topics(),
                'uptime_seconds': uptime_seconds(),
        }
    return app
app = create_app()