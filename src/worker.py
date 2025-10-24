import asyncio
import logging
from datetime import datetime


logger = logging.getLogger('worker')


class ConsumerWorker:
    def __init__(self, queue: asyncio.Queue, dedup_store, processed_events_store: list):
        self.queue = queue
        self.dedup_store = dedup_store
        self.processed_events_store = processed_events_store
        self._running = False

    async def start(self):
        self._running = True
        while self._running:
            try:
                event = await self.queue.get()
            except asyncio.CancelledError:
                break
            await self._handle(event)
            self.queue.task_done()

    async def _handle(self, event: dict):
        topic = event['topic']
        event_id = event['event_id']
        ts = datetime.utcnow().isoformat()
        inserted = self.dedup_store.mark_processed(topic, event_id, ts)
        if not inserted:
            logger.info(f"Duplicate dropped: topic={topic} event_id={event_id}")
            return
        self.processed_events_store.append({
            'topic': topic,
            'event_id': event_id,
            'processed_at': ts,
            'source': event.get('source'),
            'payload': event.get('payload'),
        })
        logger.info(f"Processed event: topic={topic} event_id={event_id}")
    
    def stop(self):
        """Stop the worker gracefully"""
        self._running = False