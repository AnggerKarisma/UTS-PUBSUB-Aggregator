import sqlite3
import threading
from typing import Tuple


class DedupStore:
    def __init__(self, path: str = 'dedup.db'):
        self.path = path
        self._lock = threading.Lock()
        self._init_db()


    def _conn(self):
        return sqlite3.connect(self.path, isolation_level=None, check_same_thread=False)


    def _init_db(self):
        with self._lock:
            conn = self._conn()
            cur = conn.cursor()
            cur.execute('''
                CREATE TABLE IF NOT EXISTS dedup (
                    topic TEXT NOT NULL,
                    event_id TEXT NOT NULL,
                    processed_at TEXT NOT NULL,
                    PRIMARY KEY(topic, event_id)
                )
           ''')
            conn.commit()
            conn.close()


    def is_processed(self, topic: str, event_id: str) -> bool:
        with self._lock:
            conn = self._conn()
            cur = conn.cursor()
            cur.execute('SELECT 1 FROM dedup WHERE topic=? AND event_id=? LIMIT 1', (topic, event_id))
            row = cur.fetchone()
            conn.close()
        return row is not None


    def mark_processed(self, topic: str, event_id: str, processed_at: str) -> bool:
        with self._lock:
            conn = self._conn()
            cur = conn.cursor()
            try:
                cur.execute('INSERT INTO dedup(topic,event_id,processed_at) VALUES (?,?,?)', (topic, event_id, processed_at))
                conn.commit()
                inserted = True
            except sqlite3.IntegrityError:
                inserted = False
            finally:
                conn.close()
            return inserted


    def list_topics(self) -> list[str]:
        with self._lock:
            conn = self._conn()
            cur = conn.cursor()
            cur.execute('SELECT DISTINCT topic FROM dedup')
            rows = [r[0] for r in cur.fetchall()]
            conn.close()
        return rows


    def count_processed(self) -> int:
        with self._lock:
            conn = self._conn()
            cur = conn.cursor()
            cur.execute('SELECT COUNT(1) FROM dedup')
            n = cur.fetchone()[0]
            conn.close()
        return n

    def list_events_for_topic(self, topic: str) -> list[Tuple[str, str]]:
        with self._lock:
            conn = self._conn()
            cur = conn.cursor()
            cur.execute('SELECT event_id, processed_at FROM dedup WHERE topic=? ORDER BY processed_at', (topic,))
            rows = cur.fetchall()
            conn.close()
        return rows