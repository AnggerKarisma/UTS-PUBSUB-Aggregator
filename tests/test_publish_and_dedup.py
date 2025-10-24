import pytest
import asyncio
from datetime import datetime


@pytest.mark.asyncio
async def test_single_event_publish(client):
    """Test publish single event"""
    event = {
        "topic": "user.created",
        "event_id": "evt-001",
        "timestamp": datetime.utcnow().isoformat(),
        "source": "user-service",
        "payload": {"user_id": "123", "email": "test@example.com"}
    }
    
    response = await client.post("/publish", json=event)
    assert response.status_code == 200
    assert response.json()["accepted"] == 1
    
    # Wait for processing
    await asyncio.sleep(0.3)
    
    # Check stats
    stats = await client.get("/stats")
    stats_data = stats.json()
    assert stats_data["received"] == 1
    assert stats_data["unique_processed"] == 1
    assert stats_data["duplicate_dropped"] == 0


@pytest.mark.asyncio
async def test_batch_event_publish(client):
    """Test publish multiple events at once"""
    events = [
        {
            "topic": "order.created",
            "event_id": f"order-{i}",
            "timestamp": datetime.utcnow().isoformat(),
            "source": "order-service",
            "payload": {"order_id": i, "amount": 100 * i}
        }
        for i in range(1, 6)
    ]
    
    response = await client.post("/publish", json=events)
    assert response.status_code == 200
    assert response.json()["accepted"] == 5
    
    # Wait for processing
    await asyncio.sleep(0.5)
    
    # Check stats
    stats = await client.get("/stats")
    stats_data = stats.json()
    assert stats_data["received"] == 5
    assert stats_data["unique_processed"] == 5


@pytest.mark.asyncio
async def test_duplicate_detection(client):
    """Test bahwa duplicate event tidak diproses ulang"""
    event = {
        "topic": "payment.processed",
        "event_id": "payment-001",
        "timestamp": datetime.utcnow().isoformat(),
        "source": "payment-service",
        "payload": {"amount": 5000}
    }
    
    # Send first time
    response1 = await client.post("/publish", json=event)
    assert response1.json()["accepted"] == 1
    
    await asyncio.sleep(0.3)
    
    # Send duplicate
    response2 = await client.post("/publish", json=event)
    assert response2.json()["accepted"] == 1
    
    await asyncio.sleep(0.3)
    
    # Check stats - should only have 1 unique processed
    stats = await client.get("/stats")
    stats_data = stats.json()
    assert stats_data["received"] == 2
    assert stats_data["unique_processed"] == 1
    assert stats_data["duplicate_dropped"] == 1


@pytest.mark.asyncio
async def test_duplicate_same_event_id_different_topic(client):
    """Test bahwa event dengan ID sama di topic berbeda dianggap berbeda"""
    event1 = {
        "topic": "topic-A",
        "event_id": "evt-001",
        "timestamp": datetime.utcnow().isoformat(),
        "source": "service-a",
        "payload": {"data": "A"}
    }
    
    event2 = {
        "topic": "topic-B",
        "event_id": "evt-001",
        "timestamp": datetime.utcnow().isoformat(),
        "source": "service-b",
        "payload": {"data": "B"}
    }
    
    await client.post("/publish", json=event1)
    await asyncio.sleep(0.2)
    await client.post("/publish", json=event2)
    await asyncio.sleep(0.2)
    
    # Both should be processed
    stats = await client.get("/stats")
    stats_data = stats.json()
    assert stats_data["unique_processed"] == 2
    assert stats_data["duplicate_dropped"] == 0


@pytest.mark.asyncio
async def test_get_events_all(client):
    """Test GET /events tanpa filter"""
    events = [
        {
            "topic": "test.event",
            "event_id": f"evt-{i}",
            "timestamp": datetime.utcnow().isoformat(),
            "source": "test-service",
            "payload": {"index": i}
        }
        for i in range(3)
    ]
    
    await client.post("/publish", json=events)
    await asyncio.sleep(0.5)
    
    response = await client.get("/events")
    events_data = response.json()
    assert len(events_data) == 3
    assert all("event_id" in e for e in events_data)


@pytest.mark.asyncio
async def test_get_events_by_topic(client):
    """Test GET /events dengan filter topic"""
    events = [
        {
            "topic": "topic-X",
            "event_id": "evt-1",
            "timestamp": datetime.utcnow().isoformat(),
            "source": "service",
            "payload": {}
        },
        {
            "topic": "topic-Y",
            "event_id": "evt-2",
            "timestamp": datetime.utcnow().isoformat(),
            "source": "service",
            "payload": {}
        },
        {
            "topic": "topic-X",
            "event_id": "evt-3",
            "timestamp": datetime.utcnow().isoformat(),
            "source": "service",
            "payload": {}
        }
    ]
    
    await client.post("/publish", json=events)
    await asyncio.sleep(0.5)
    
    # Get only topic-X events
    response = await client.get("/events?topic=topic-X")
    events_data = response.json()
    assert len(events_data) == 2
    assert all(e["event_id"] in ["evt-1", "evt-3"] for e in events_data)