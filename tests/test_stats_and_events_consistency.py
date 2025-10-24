import pytest
import asyncio
from datetime import datetime


@pytest.mark.asyncio
async def test_stats_consistency(client):
    """Test bahwa stats endpoint mengembalikan data yang konsisten"""
    # Kirim 10 event unique
    events = [
        {
            "topic": "test.event",
            "event_id": f"evt-{i}",
            "timestamp": datetime.utcnow().isoformat(),
            "source": "test-service",
            "payload": {"index": i}
        }
        for i in range(10)
    ]
    
    await client.post("/publish", json=events)
    await asyncio.sleep(0.5)
    
    # Kirim 3 duplicate
    duplicates = events[:3]
    await client.post("/publish", json=duplicates)
    await asyncio.sleep(0.5)
    
    # Check stats
    stats_response = await client.get("/stats")
    stats = stats_response.json()
    
    # Verify calculations
    assert stats["received"] == 13  # 10 + 3
    assert stats["unique_processed"] == 10
    assert stats["duplicate_dropped"] == 3
    assert stats["received"] == stats["unique_processed"] + stats["duplicate_dropped"]
    assert "topics" in stats
    assert "uptime_seconds" in stats
    assert stats["uptime_seconds"] > 0


@pytest.mark.asyncio
async def test_stats_topics_list(client):
    """Test bahwa topics list di stats akurat"""
    # Kirim event ke 3 topic berbeda
    topics = ["topic.A", "topic.B", "topic.C"]
    
    for topic in topics:
        event = {
            "topic": topic,
            "event_id": f"evt-{topic}",
            "timestamp": datetime.utcnow().isoformat(),
            "source": "test-service",
            "payload": {}
        }
        await client.post("/publish", json=event)
    
    await asyncio.sleep(0.5)
    
    stats_response = await client.get("/stats")
    stats = stats_response.json()
    
    assert len(stats["topics"]) == 3
    assert set(stats["topics"]) == set(topics)


@pytest.mark.asyncio
async def test_events_endpoint_consistency(client):
    """Test bahwa /events endpoint konsisten dengan /stats"""
    # Kirim beberapa event
    events = [
        {
            "topic": "order.created",
            "event_id": f"order-{i}",
            "timestamp": datetime.utcnow().isoformat(),
            "source": "order-service",
            "payload": {"order_id": i}
        }
        for i in range(5)
    ]
    
    await client.post("/publish", json=events)
    await asyncio.sleep(0.5)
    
    # Get stats
    stats_response = await client.get("/stats")
    stats = stats_response.json()
    
    # Get events
    events_response = await client.get("/events")
    events_data = events_response.json()
    
    # Verify consistency
    assert len(events_data) == stats["unique_processed"]


@pytest.mark.asyncio
async def test_events_by_topic_consistency(client):
    """Test bahwa /events?topic=X konsisten dengan data yang dikirim"""
    # Kirim 3 event ke topic-A dan 2 event ke topic-B
    topic_a_events = [
        {
            "topic": "topic-A",
            "event_id": f"a-{i}",
            "timestamp": datetime.utcnow().isoformat(),
            "source": "service-a",
            "payload": {}
        }
        for i in range(3)
    ]
    
    topic_b_events = [
        {
            "topic": "topic-B",
            "event_id": f"b-{i}",
            "timestamp": datetime.utcnow().isoformat(),
            "source": "service-b",
            "payload": {}
        }
        for i in range(2)
    ]
    
    await client.post("/publish", json=topic_a_events)
    await client.post("/publish", json=topic_b_events)
    await asyncio.sleep(0.5)
    
    # Get events by topic
    response_a = await client.get("/events?topic=topic-A")
    response_b = await client.get("/events?topic=topic-B")
    
    events_a = response_a.json()
    events_b = response_b.json()
    
    assert len(events_a) == 3
    assert len(events_b) == 2
    
    # Verify event_ids
    event_ids_a = [e["event_id"] for e in events_a]
    assert set(event_ids_a) == {"a-0", "a-1", "a-2"}


@pytest.mark.asyncio
async def test_uptime_increments(client):
    """Test bahwa uptime_seconds selalu bertambah"""
    # Get first uptime
    stats1 = await client.get("/stats")
    uptime1 = stats1.json()["uptime_seconds"]
    
    # Wait a bit
    await asyncio.sleep(1)
    
    # Get second uptime
    stats2 = await client.get("/stats")
    uptime2 = stats2.json()["uptime_seconds"]
    
    # Uptime should have increased
    assert uptime2 > uptime1
    assert uptime2 - uptime1 >= 1


@pytest.mark.asyncio
async def test_concurrent_stats_access(client):
    """Test bahwa multiple concurrent requests ke /stats tidak menyebabkan race condition"""
    # Kirim beberapa event
    events = [
        {
            "topic": "test.concurrent",
            "event_id": f"evt-{i}",
            "timestamp": datetime.utcnow().isoformat(),
            "source": "test-service",
            "payload": {}
        }
        for i in range(10)
    ]
    
    await client.post("/publish", json=events)
    await asyncio.sleep(0.5)
    
    # Make multiple concurrent stats requests
    tasks = [client.get("/stats") for _ in range(10)]
    responses = await asyncio.gather(*tasks)
    
    # All responses should return the same data
    stats_data = [r.json() for r in responses]
    unique_processed = [s["unique_processed"] for s in stats_data]
    
    # All should report the same number
    assert len(set(unique_processed)) == 1
    assert unique_processed[0] == 10