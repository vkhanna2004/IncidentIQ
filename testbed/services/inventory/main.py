import os
import json
import logging
from contextlib import asynccontextmanager
from typing import Any, Dict, List

from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field

import redis.asyncio as redis
from redis.exceptions import RedisError

import psycopg_pool
from psycopg.rows import dict_row

from opentelemetry import trace
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

from testbed.common.telemetry import setup_telemetry

logger = logging.getLogger(__name__)
tracer = trace.get_tracer("inventory")

# Postgres Configuration
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "incidents")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")

# Redis Configuration
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

# Fault Simulation flags
MEMORY_LEAK_ENABLED = os.getenv("MEMORY_LEAK_ENABLED", "false").lower() == "true"
MEMORY_LEAK_CHUNK_KB = int(os.getenv("MEMORY_LEAK_CHUNK_KB", "1024"))  # 1MB per request
_leak_storage: List[bytes] = []

DEFAULT_PRODUCTS = [
    {"item_id": "prod_laptop", "name": "Developer Laptop", "price": 1299.99, "stock": 5000},
    {"item_id": "prod_mouse", "name": "Ergonomic Mouse", "price": 49.99, "stock": 10000},
    {"item_id": "prod_keyboard", "name": "Mechanical Keyboard", "price": 119.99, "stock": 8000},
    {"item_id": "prod_monitor", "name": "4K Ultra HD Monitor", "price": 399.99, "stock": 3000},
]

redis_client: redis.Redis | None = None
db_pool: psycopg_pool.AsyncConnectionPool | None = None


def get_conninfo() -> str:
    return (
        f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@"
        f"{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    )


async def init_storage() -> None:
    """Initialize Postgres tables and seed initial inventory; hydrate Redis cache."""
    # 1. Postgres schema
    if db_pool:
        try:
            async with db_pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("""
                        CREATE TABLE IF NOT EXISTS inventory (
                            item_id TEXT PRIMARY KEY,
                            name TEXT NOT NULL,
                            price DOUBLE PRECISION NOT NULL,
                            stock INT NOT NULL
                        );
                    """)
                    for item in DEFAULT_PRODUCTS:
                        await cur.execute("""
                            INSERT INTO inventory (item_id, name, price, stock)
                            VALUES (%s, %s, %s, %s)
                            ON CONFLICT (item_id) DO NOTHING;
                        """, (item["item_id"], item["name"], item["price"], item["stock"]))
                    await conn.commit()
            logger.info("Postgres inventory table initialized and seeded")
        except Exception as exc:
            logger.warning("Could not initialize DB schema for inventory: %s", str(exc))

    # 2. Redis cache seed
    if redis_client:
        try:
            for item in DEFAULT_PRODUCTS:
                await redis_client.set(f"product:{item['item_id']}", json.dumps(item))
                await redis_client.set(f"stock:{item['item_id']}", str(item["stock"]))
            logger.info("Redis inventory cache hydrated successfully")
        except Exception as exc:
            logger.warning("Redis hydration failed (cache outage or offline): %s", str(exc))


@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_client, db_pool
    setup_telemetry("inventory")

    # Initialize Redis connection with aggressive socket timeouts
    redis_client = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        socket_timeout=1.0,
        socket_connect_timeout=1.0,
        decode_responses=True
    )

    # Initialize Postgres pool for fallback
    db_pool = psycopg_pool.AsyncConnectionPool(
        conninfo=get_conninfo(),
        min_size=2,
        max_size=10,
        timeout=5.0,
        kwargs={"row_factory": dict_row}
    )
    await db_pool.open()

    await init_storage()
    logger.info("Inventory service initialized (memory_leak=%s)", MEMORY_LEAK_ENABLED)
    yield

    if redis_client:
        await redis_client.aclose()
    if db_pool:
        await db_pool.close()
    logger.info("Inventory service shutdown complete")


app = FastAPI(title="Inventory Service", lifespan=lifespan)
FastAPIInstrumentor.instrument_app(app)


class ReserveRequest(BaseModel):
    item_id: str = Field(..., description="Item ID to reserve")
    quantity: int = Field(default=1, ge=1, description="Quantity")
    order_id: str = Field(..., description="Order ID")


@app.get("/health")
async def health() -> Dict[str, Any]:
    redis_ok = False
    db_ok = False

    if redis_client:
        try:
            await redis_client.ping()
            redis_ok = True
        except Exception:
            pass

    if db_pool:
        try:
            async with db_pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("SELECT 1")
            db_ok = True
        except Exception:
            pass

    return {
        "status": "ok" if (redis_ok or db_ok) else "degraded",
        "service": "inventory",
        "redis_connected": redis_ok,
        "postgres_connected": db_ok,
        "memory_leak_active": MEMORY_LEAK_ENABLED,
        "retained_chunks": len(_leak_storage)
    }


@app.get("/products")
async def get_products() -> List[Dict[str, Any]]:
    # Trigger memory leak growth if scenario flag enabled
    if MEMORY_LEAK_ENABLED:
        _leak_storage.append(b"M" * (MEMORY_LEAK_CHUNK_KB * 1024))
        logger.warning("Memory leak simulation: allocated %d KB (total chunks: %d)",
                       MEMORY_LEAK_CHUNK_KB, len(_leak_storage))

    with tracer.start_as_current_span("inventory_fetch_products"):
        # Try Redis cache first
        if redis_client:
            try:
                keys = await redis_client.keys("product:*")
                if keys:
                    products = []
                    for k in sorted(keys):
                        val = await redis_client.get(k)
                        if val:
                            products.append(json.loads(val))
                    if products:
                        return products
            except (RedisError, ConnectionError, OSError, TimeoutError) as exc:
                logger.warning("Redis cache unavailable (%s), falling back to Postgres database", str(exc))

        # Fallback to Postgres database
        if not db_pool:
            raise HTTPException(status_code=503, detail="Storage unavailable")

        try:
            with tracer.start_as_current_span("db_fetch_products_fallback"):
                async with db_pool.connection() as conn:
                    async with conn.cursor() as cur:
                        await cur.execute("SELECT item_id, name, price, stock FROM inventory ORDER BY item_id")
                        rows = await cur.fetchall()
                        return [dict(r) for r in rows]
        except Exception as exc:
            logger.error("Database query failed while fetching products: %s", str(exc))
            raise HTTPException(status_code=500, detail="Database error") from exc


@app.post("/reserve")
async def reserve_stock(request: ReserveRequest) -> Dict[str, Any]:
    # Trigger memory leak growth if enabled
    if MEMORY_LEAK_ENABLED:
        _leak_storage.append(b"M" * (MEMORY_LEAK_CHUNK_KB * 1024))
        logger.warning("Memory leak simulation: allocated %d KB (total chunks: %d)",
                       MEMORY_LEAK_CHUNK_KB, len(_leak_storage))

    with tracer.start_as_current_span("inventory_reserve_stock"):
        logger.info("Reserving %d of item %s for order %s",
                    request.quantity, request.item_id, request.order_id)

        # 1. Attempt reservation in Redis
        if redis_client:
            try:
                stock_key = f"stock:{request.item_id}"
                remaining = await redis_client.decrby(stock_key, request.quantity)
                if remaining < 0:
                    # Rollback Redis decrement
                    await redis_client.incrby(stock_key, request.quantity)
                    logger.warning("Insufficient stock in Redis for item %s", request.item_id)
                    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Insufficient stock")

                # Asynchronously update DB to maintain sync
                if db_pool:
                    try:
                        async with db_pool.connection() as conn:
                            async with conn.cursor() as cur:
                                await cur.execute(
                                    "UPDATE inventory SET stock = stock - %s WHERE item_id = %s",
                                    (request.quantity, request.item_id)
                                )
                                await conn.commit()
                    except Exception as db_exc:
                        logger.warning("Async DB stock sync failed: %s", str(db_exc))

                return {
                    "status": "reserved",
                    "item_id": request.item_id,
                    "quantity": request.quantity,
                    "remaining_stock": remaining,
                    "source": "redis_cache"
                }
            except (RedisError, ConnectionError, OSError, TimeoutError) as exc:
                logger.warning("Redis reservation failed (%s), falling back to Postgres database", str(exc))

        # 2. Fallback reservation in Postgres
        if not db_pool:
            raise HTTPException(status_code=503, detail="Storage unavailable")

        try:
            with tracer.start_as_current_span("db_reserve_stock_fallback"):
                async with db_pool.connection() as conn:
                    async with conn.cursor() as cur:
                        await cur.execute(
                            """
                            UPDATE inventory
                            SET stock = stock - %s
                            WHERE item_id = %s AND stock >= %s
                            RETURNING stock
                            """,
                            (request.quantity, request.item_id, request.quantity)
                        )
                        row = await cur.fetchone()
                        if not row:
                            logger.warning("Insufficient stock in DB for item %s", request.item_id)
                            raise HTTPException(
                                status_code=status.HTTP_400_BAD_REQUEST,
                                detail="Insufficient stock or item not found"
                            )
                        await conn.commit()
                        remaining = row["stock"]

                return {
                    "status": "reserved",
                    "item_id": request.item_id,
                    "quantity": request.quantity,
                    "remaining_stock": remaining,
                    "source": "postgres_database"
                }
        except HTTPException:
            raise
        except Exception as exc:
            logger.error("Postgres reservation query error: %s", str(exc))
            raise HTTPException(status_code=500, detail="Database reservation error") from exc


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("testbed.services.inventory.main:app", host="0.0.0.0", port=8003, reload=True)
