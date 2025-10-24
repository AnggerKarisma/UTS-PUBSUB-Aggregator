import pytest
import os
import tempfile
import asyncio
from datetime import datetime
from httpx import AsyncClient, ASGITransport
from src.main import create_app
from src.dedup_store import DedupStore


@pytest.mark.asyncio
async def test_persistence_after_restart():
    """
    Test bahwa data tetap persisten setelah aplikasi di-restart.
    Menguji apakah event yang sudah diproses sebelum restart tidak akan diproses ulang.
    """
    with tempfile.NamedTemporaryFile(delete=False, suffix='.db') as tmp:
        db_path = tmp.name
    
    try:
        os.environ['DEDUP_DB_PATH'] = db_path
        
        app1 = create_app()
        
        async with app1.router.lifespan_context(app1):
            transport1 = ASGITransport(app=app1)
            async with AsyncClient(transport=transport1, base_url="http://test") as client:
                events_batch1 = [
                    {
                        "topic": "user.login",
                        "event_id": "evt-001",
                        "timestamp": datetime.utcnow().isoformat(),
                        "source": "auth-service",
                        "payload": {"user_id": "user123"}
                    },
                    {
                        "topic": "user.login",
                        "event_id": "evt-002",
                        "timestamp": datetime.utcnow().isoformat(),
                        "source": "auth-service",
                        "payload": {"user_id": "user456"}
                    }
                ]
                
                response = await client.post("/publish", json=events_batch1)
                assert response.status_code == 200
                assert response.json()["accepted"] == 2
                
                await asyncio.sleep(0.5)
                
                stats1 = await client.get("/stats")
                assert stats1.json()["unique_processed"] == 2
        
        store = DedupStore(db_path)
        assert store.count_processed() == 2, "Database should have 2 events after first session"
        assert store.is_processed("user.login", "evt-001"), "evt-001 should be in database"
        assert store.is_processed("user.login", "evt-002"), "evt-002 should be in database"
        
        # === FASE 2: Restart aplikasi ===
        app2 = create_app()
        
        async with app2.router.lifespan_context(app2):
            transport2 = ASGITransport(app=app2)
            async with AsyncClient(transport=transport2, base_url="http://test") as client:
                # Kirim event yang sama (duplicate) dan event baru
                events_batch2 = [
                    {
                        "topic": "user.login",
                        "event_id": "evt-001",  # Duplicate
                        "timestamp": datetime.utcnow().isoformat(),
                        "source": "auth-service",
                        "payload": {"user_id": "user123"}
                    },
                    {
                        "topic": "user.login",
                        "event_id": "evt-003",  # New
                        "timestamp": datetime.utcnow().isoformat(),
                        "source": "auth-service",
                        "payload": {"user_id": "user789"}
                    }
                ]
                
                response = await client.post("/publish", json=events_batch2)
                assert response.status_code == 200
                assert response.json()["accepted"] == 2
                
                # Tunggu event diproses
                await asyncio.sleep(0.5)
                
                stats2 = await client.get("/stats")
                stats_data = stats2.json()
                
                assert stats_data["unique_processed"] == 3, "Should have 3 unique events total in database"
                
                assert stats_data["received"] == 2, "Should have received 2 events in this session"
                
              
                store2 = DedupStore(db_path)
                assert store2.count_processed() == 3, "Database should have exactly 3 unique events"
                assert store2.is_processed("user.login", "evt-001"), "evt-001 should still be in database"
                assert store2.is_processed("user.login", "evt-002"), "evt-002 should still be in database"
                assert store2.is_processed("user.login", "evt-003"), "evt-003 should be in database"
                
                processed_events = stats_data.get("unique_processed")
                
                
                print(f"\n=== Verification ===")
                print(f"Total unique events in DB: {store2.count_processed()}")
                print(f"Events received in session 2: {stats_data['received']}")
                print(f"Total unique processed (from DB): {stats_data['unique_processed']}")
        
    finally:
        if os.path.exists(db_path):
            os.remove(db_path)
        if 'DEDUP_DB_PATH' in os.environ:
            del os.environ['DEDUP_DB_PATH']


@pytest.mark.asyncio
async def test_database_integrity():
    """Test integritas database setelah multiple operations"""
    with tempfile.NamedTemporaryFile(delete=False, suffix='.db') as tmp:
        db_path = tmp.name
    
    try:
        store = DedupStore(db_path)
        
        # Insert beberapa event
        assert store.mark_processed("topic1", "evt1", "2025-01-01T00:00:00")
        assert store.mark_processed("topic1", "evt2", "2025-01-01T00:00:01")
        assert store.mark_processed("topic2", "evt1", "2025-01-01T00:00:02")
        
        # Cek duplikasi
        assert not store.mark_processed("topic1", "evt1", "2025-01-01T00:00:03")
        
        # Verify counts
        assert store.count_processed() == 3
        assert len(store.list_topics()) == 2
        
        # Verify topic-specific queries
        topic1_events = store.list_events_for_topic("topic1")
        assert len(topic1_events) == 2
        
    finally:
        if os.path.exists(db_path):
            os.remove(db_path)