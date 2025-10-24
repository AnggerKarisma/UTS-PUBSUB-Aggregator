import pytest
from datetime import datetime


@pytest.mark.asyncio
async def test_valid_event_schema(client):
    """Test bahwa event dengan schema valid diterima"""
    valid_event = {
        "topic": "user.registered",
        "event_id": "evt-valid-001",
        "timestamp": datetime.utcnow().isoformat(),
        "source": "auth-service",
        "payload": {
            "user_id": "12345",
            "email": "user@example.com",
            "created_at": datetime.utcnow().isoformat()
        }
    }
    
    response = await client.post("/publish", json=valid_event)
    assert response.status_code == 200
    assert response.json()["accepted"] == 1


@pytest.mark.asyncio
async def test_missing_required_field(client):
    """Test bahwa event tanpa field required ditolak"""
    # Missing 'source' field
    invalid_event = {
        "topic": "user.registered",
        "event_id": "evt-001",
        "timestamp": datetime.utcnow().isoformat(),
        "payload": {"data": "test"}
    }
    
    response = await client.post("/publish", json=invalid_event)
    assert response.status_code == 422  # Validation error


@pytest.mark.asyncio
async def test_empty_topic(client):
    """Test bahwa topic tidak boleh kosong"""
    invalid_event = {
        "topic": "",  # Empty topic
        "event_id": "evt-001",
        "timestamp": datetime.utcnow().isoformat(),
        "source": "test-service",
        "payload": {}
    }
    
    response = await client.post("/publish", json=invalid_event)
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_empty_event_id(client):
    """Test bahwa event_id tidak boleh kosong"""
    invalid_event = {
        "topic": "test.topic",
        "event_id": "",  # Empty event_id
        "timestamp": datetime.utcnow().isoformat(),
        "source": "test-service",
        "payload": {}
    }
    
    response = await client.post("/publish", json=invalid_event)
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_invalid_timestamp_format(client):
    """Test bahwa timestamp dengan format invalid ditolak"""
    invalid_event = {
        "topic": "test.topic",
        "event_id": "evt-001",
        "timestamp": "not-a-valid-timestamp",
        "source": "test-service",
        "payload": {}
    }
    
    response = await client.post("/publish", json=invalid_event)
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_payload_can_be_complex(client):
    """Test bahwa payload bisa berisi struktur data kompleks"""
    complex_event = {
        "topic": "order.created",
        "event_id": "order-001",
        "timestamp": datetime.utcnow().isoformat(),
        "source": "order-service",
        "payload": {
            "order_id": "ORD-001",
            "customer": {
                "id": "CUST-123",
                "name": "John Doe",
                "email": "john@example.com"
            },
            "items": [
                {"sku": "ITEM-1", "quantity": 2, "price": 10.99},
                {"sku": "ITEM-2", "quantity": 1, "price": 25.50}
            ],
            "total": 47.48,
            "metadata": {
                "ip_address": "192.168.1.1",
                "user_agent": "Mozilla/5.0"
            }
        }
    }
    
    response = await client.post("/publish", json=complex_event)
    assert response.status_code == 200
    assert response.json()["accepted"] == 1


@pytest.mark.asyncio
async def test_batch_with_mixed_valid_invalid(client):
    """Test bahwa batch dengan mixed valid/invalid events ditangani dengan benar"""
    events = [
        {
            "topic": "valid.event",
            "event_id": "evt-1",
            "timestamp": datetime.utcnow().isoformat(),
            "source": "service",
            "payload": {}
        },
        {
            "topic": "",  # Invalid - empty topic
            "event_id": "evt-2",
            "timestamp": datetime.utcnow().isoformat(),
            "source": "service",
            "payload": {}
        }
    ]
    
    response = await client.post("/publish", json=events)
    # Entire batch should fail validation
    assert response.status_code == 422