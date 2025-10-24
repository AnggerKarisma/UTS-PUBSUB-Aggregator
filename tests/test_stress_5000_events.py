import pytest
import asyncio
import time
from datetime import datetime


@pytest.mark.asyncio
async def test_stress_5000_events(client):
    """
    Stress test: Kirim 5000 event dan verifikasi semua diproses dengan benar.
    Test ini mengukur throughput dan latency sistem.
    """
    total_events = 5000
    batch_size = 100  # Kirim dalam batch untuk efisiensi
    
    print(f"\n=== Stress Test: {total_events} Events ===")
    start_time = time.time()
    
    # Generate dan kirim events dalam batch
    for batch_num in range(total_events // batch_size):
        batch_events = [
            {
                "topic": f"stress.test.topic{i % 10}",  # 10 topics berbeda
                "event_id": f"evt-{batch_num * batch_size + i}",
                "timestamp": datetime.utcnow().isoformat(),
                "source": "stress-test-service",
                "payload": {
                    "batch": batch_num,
                    "index": i,
                    "data": f"payload-data-{batch_num}-{i}"
                }
            }
            for i in range(batch_size)
        ]
        
        response = await client.post("/publish", json=batch_events)
        assert response.status_code == 200
        assert response.json()["accepted"] == batch_size
        
        # Log progress setiap 10 batch
        if (batch_num + 1) % 10 == 0:
            print(f"Sent {(batch_num + 1) * batch_size} events...")
    
    publish_time = time.time() - start_time
    print(f"All events published in {publish_time:.2f} seconds")
    print(f"Publish throughput: {total_events / publish_time:.2f} events/sec")
    
    # Tunggu semua event diproses
    print("Waiting for processing to complete...")
    max_wait = 60  # Maximum 60 detik
    wait_start = time.time()
    
    while time.time() - wait_start < max_wait:
        stats_response = await client.get("/stats")
        stats = stats_response.json()
        
        if stats["unique_processed"] == total_events:
            break
        
        await asyncio.sleep(0.5)
    
    processing_time = time.time() - start_time
    
    # Verify hasil
    stats_response = await client.get("/stats")
    stats = stats_response.json()
    
    print(f"\n=== Results ===")
    print(f"Total processing time: {processing_time:.2f} seconds")
    print(f"Overall throughput: {total_events / processing_time:.2f} events/sec")
    print(f"Events received: {stats['received']}")
    print(f"Events processed: {stats['unique_processed']}")
    print(f"Duplicates dropped: {stats['duplicate_dropped']}")
    print(f"Number of topics: {len(stats['topics'])}")
    print(f"Average latency: {processing_time / total_events * 1000:.2f} ms/event")
    
    # Assertions
    assert stats["received"] == total_events
    assert stats["unique_processed"] == total_events
    assert stats["duplicate_dropped"] == 0
    assert len(stats["topics"]) == 10  # 10 different topics
    
    # Performance assertion - semua event harus diproses dalam 60 detik
    assert processing_time < max_wait, f"Processing took too long: {processing_time:.2f}s"


@pytest.mark.asyncio
async def test_stress_with_duplicates(client):
    """
    Stress test dengan duplicate detection:
    Kirim 2000 unique events, kemudian kirim 2000 duplicates.
    Total 4000 events, tapi hanya 2000 yang harus diproses.
    """
    unique_count = 2000
    batch_size = 100
    
    print(f"\n=== Stress Test with Duplicates ===")
    print(f"Unique events: {unique_count}")
    print(f"Total events (with duplicates): {unique_count * 2}")
    
    start_time = time.time()
    
    # Fase 1: Kirim unique events
    print("Phase 1: Sending unique events...")
    unique_events = []
    for batch_num in range(unique_count // batch_size):
        batch = [
            {
                "topic": f"dedup.test.topic{i % 5}",
                "event_id": f"evt-{batch_num * batch_size + i}",
                "timestamp": datetime.utcnow().isoformat(),
                "source": "dedup-test",
                "payload": {"index": batch_num * batch_size + i}
            }
            for i in range(batch_size)
        ]
        unique_events.extend(batch)
        await client.post("/publish", json=batch)
    
    await asyncio.sleep(2)  # Wait for processing
    
    # Fase 2: Kirim duplicate events
    print("Phase 2: Sending duplicate events...")
    for batch_num in range(unique_count // batch_size):
        batch = unique_events[batch_num * batch_size:(batch_num + 1) * batch_size]
        await client.post("/publish", json=batch)
    
    await asyncio.sleep(3)  # Wait for processing
    
    total_time = time.time() - start_time
    
    # Verify results
    stats_response = await client.get("/stats")
    stats = stats_response.json()
    
    print(f"\n=== Results ===")
    print(f"Total time: {total_time:.2f} seconds")
    print(f"Events received: {stats['received']}")
    print(f"Unique processed: {stats['unique_processed']}")
    print(f"Duplicates dropped: {stats['duplicate_dropped']}")
    print(f"Dedup rate: {stats['duplicate_dropped'] / stats['received'] * 100:.1f}%")
    
    assert stats["received"] == unique_count * 2
    assert stats["unique_processed"] == unique_count
    assert stats["duplicate_dropped"] == unique_count


@pytest.mark.asyncio
async def test_concurrent_publish_stress(client):
    """
    Test concurrent publishing dari multiple "clients".
    Simulasi 10 concurrent publishers yang masing-masing mengirim 500 events.
    """
    num_publishers = 10
    events_per_publisher = 500
    total_expected = num_publishers * events_per_publisher
    
    print(f"\n=== Concurrent Publishing Test ===")
    print(f"Publishers: {num_publishers}")
    print(f"Events per publisher: {events_per_publisher}")
    print(f"Total events: {total_expected}")
    
    async def publisher(publisher_id: int):
        """Simulate a single publisher"""
        events = [
            {
                "topic": f"concurrent.publisher{publisher_id}",
                "event_id": f"pub{publisher_id}-evt-{i}",
                "timestamp": datetime.utcnow().isoformat(),
                "source": f"publisher-{publisher_id}",
                "payload": {"publisher": publisher_id, "index": i}
            }
            for i in range(events_per_publisher)
        ]
        
        # Kirim dalam batch kecil untuk simulasi real-world
        batch_size = 50
        for i in range(0, len(events), batch_size):
            batch = events[i:i + batch_size]
            await client.post("/publish", json=batch)
            await asyncio.sleep(0.01)  # Small delay antara batch
    
    start_time = time.time()
    
    # Run all publishers concurrently
    await asyncio.gather(*[publisher(i) for i in range(num_publishers)])
    
    publish_time = time.time() - start_time
    print(f"All events published in {publish_time:.2f} seconds")
    
    # Wait for processing
    await asyncio.sleep(5)
    
    total_time = time.time() - start_time
    
    # Verify results
    stats_response = await client.get("/stats")
    stats = stats_response.json()
    
    print(f"\n=== Results ===")
    print(f"Total time: {total_time:.2f} seconds")
    print(f"Events received: {stats['received']}")
    print(f"Events processed: {stats['unique_processed']}")
    print(f"Throughput: {stats['unique_processed'] / total_time:.2f} events/sec")
    print(f"Number of topics: {len(stats['topics'])}")
    
    assert stats["received"] == total_expected
    assert stats["unique_processed"] == total_expected
    assert len(stats["topics"]) == num_publishers


@pytest.mark.asyncio
async def test_memory_efficiency(client):
    """
    Test memory efficiency dengan large payload.
    Kirim 1000 events dengan payload yang relatif besar.
    """
    num_events = 1000
    
    # Create large payload (simulate real-world data)
    large_payload = {
        "user_data": {
            "id": "user-12345",
            "profile": {
                "name": "Test User",
                "email": "test@example.com",
                "preferences": {f"pref_{i}": f"value_{i}" for i in range(50)}
            }
        },
        "transaction_history": [
            {"tx_id": f"tx-{i}", "amount": 100 * i, "timestamp": datetime.utcnow().isoformat()}
            for i in range(20)
        ],
        "metadata": {
            "ip": "192.168.1.1",
            "user_agent": "Mozilla/5.0 (compatible)",
            "session_id": "sess-xyz-" + "0" * 100  # Padding untuk ukuran
        }
    }
    
    print(f"\n=== Memory Efficiency Test ===")
    print(f"Events: {num_events}")
    print(f"Approximate payload size: ~1-2 KB per event")
    
    start_time = time.time()
    
    # Kirim events dengan large payload
    batch_size = 50
    for batch_num in range(num_events // batch_size):
        batch = [
            {
                "topic": "large.payload.test",
                "event_id": f"large-evt-{batch_num * batch_size + i}",
                "timestamp": datetime.utcnow().isoformat(),
                "source": "memory-test",
                "payload": large_payload
            }
            for i in range(batch_size)
        ]
        
        response = await client.post("/publish", json=batch)
        assert response.status_code == 200
    
    # Wait for processing
    await asyncio.sleep(3)
    
    total_time = time.time() - start_time
    
    # Verify
    stats_response = await client.get("/stats")
    stats = stats_response.json()
    
    print(f"\n=== Results ===")
    print(f"Total time: {total_time:.2f} seconds")
    print(f"Events processed: {stats['unique_processed']}")
    print(f"Processing rate: {stats['unique_processed'] / total_time:.2f} events/sec")
    
    assert stats["unique_processed"] == num_events
    
    # Verify data integrity - get events and check payload
    events_response = await client.get("/events")
    events = events_response.json()
    
    # Sample check - verify beberapa events masih memiliki payload lengkap
    assert len(events) == num_events
    assert events[0]["payload"]["metadata"]["session_id"].startswith("sess-xyz-")