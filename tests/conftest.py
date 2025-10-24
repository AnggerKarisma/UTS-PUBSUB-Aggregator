import pytest
import pytest_asyncio
import asyncio
import os
import tempfile
from httpx import AsyncClient, ASGITransport
from src.main import create_app


@pytest_asyncio.fixture(scope="function")
async def client():
    """
    Fixture untuk membuat AsyncClient yang terhubung ke FastAPI app.
    Menggunakan ASGITransport untuk testing.
    Setiap test mendapat database temporary yang isolated.
    """
    # Create temporary database untuk setiap test
    with tempfile.NamedTemporaryFile(delete=False, suffix='.db') as tmp:
        db_path = tmp.name
    
    # Set environment variable
    old_db_path = os.environ.get('DEDUP_DB_PATH')
    os.environ['DEDUP_DB_PATH'] = db_path
    
    try:
        app = create_app()
        
        # Start lifespan context
        async with app.router.lifespan_context(app):
            # Create client with ASGI transport
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as ac:
                yield ac
    finally:
        # Cleanup: remove temporary database
        if os.path.exists(db_path):
            try:
                os.remove(db_path)
            except:
                pass
        
        # Restore original environment
        if old_db_path is not None:
            os.environ['DEDUP_DB_PATH'] = old_db_path
        elif 'DEDUP_DB_PATH' in os.environ:
            del os.environ['DEDUP_DB_PATH']


@pytest.fixture(scope="session")
def event_loop():
    """
    Create an instance of the default event loop for the test session.
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
    yield loop
    loop.close()