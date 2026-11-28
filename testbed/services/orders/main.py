import os
import uuid
import logging
from contextlib import asynccontextmanager
from typing import Any, Dict

import httpx
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field

import psycopg_pool
from psycopg.rows import dict_row

from opentelemetry import trace, metrics
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor

from testbed.common.telemetry import setup_telemetry

logger = logging.getLogger(__name__)

# Service & Database Config
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "incidents")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")

# Connection pool settings (DB_POOL_SIZE can be throttled during faults)
DB_POOL_SIZE = int(os.getenv("DB_POOL_SIZE", "10"))
DB_TIMEOUT = float(os.getenv("DB_TIMEOUT", "5.0"))

PAYMENTS_SERVICE_URL = os.getenv("PAYMENTS_SERVICE_URL", "http://localhost:8002")
INVENTORY_SERVICE_URL = os.getenv("INVENTORY_SERVICE_URL", "http://localhost:8003")

# Downstream timeout (can be made tiny in bad_config scenario)
PAYMENTS_TIMEOUT_MS = float(os.getenv("PAYMENTS_TIMEOUT_MS", "5000"))

tracer = trace.get_tracer("orders")
meter = metrics.get_meter("orders")

# Custom metrics
orders_created_counter = meter.create_counter(
    name="orders_created_total",
    description="Total number of successfully placed orders",
    unit="1"
)
order_errors_counter = meter.create_counter(
    name="order_errors_total",
    description="Total number of failed order attempts",
    unit="1"
)

db_pool: psycopg_pool.AsyncConnectionPool | None = None
http_client: httpx.AsyncClient | None = None


def get_conninfo() -> str:
    return (
        f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@"
        f"{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    )


async def init_db(pool: psycopg_pool.AsyncConnectionPool) -> None:
    """Create orders table and index if they do not exist."""
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                CREATE TABLE IF NOT EXISTS orders (
                    id TEXT PRIMARY KEY,
                    customer_id TEXT NOT NULL,
                    item_id TEXT NOT NULL,
                    quantity INT NOT NULL,
                    amount DOUBLE PRECISION NOT NULL,
                    status TEXT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                CREATE INDEX IF NOT EXISTS idx_orders_customer ON orders(customer_id);
                CREATE INDEX IF NOT EXISTS idx_orders_created ON orders(created_at);
            """)
            await conn.commit()
    logger.info("Database schema initialized for orders")


@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_pool, http_client
    setup_telemetry("orders")
    HTTPXClientInstrumentor().instrument()

    http_client = httpx.AsyncClient(timeout=10.0)

    conninfo = get_conninfo()
    logger.info("Initializing Postgres connection pool (min=2, max=%d, host=%s)",
                DB_POOL_SIZE, POSTGRES_HOST)
    db_pool = psycopg_pool.AsyncConnectionPool(
        conninfo=conninfo,
        min_size=min(2, DB_POOL_SIZE),
        max_size=DB_POOL_SIZE,
        timeout=DB_TIMEOUT,
        kwargs={"row_factory": dict_row}
    )
    await db_pool.open()

    try:
        await init_db(db_pool)
    except Exception as exc:
        logger.warning("Could not initialize DB schema at startup (will retry on requests): %s", str(exc))

    yield

    if http_client:
        await http_client.aclose()
    if db_pool:
        await db_pool.close()
    logger.info("Orders service shutdown complete")


app = FastAPI(title="Orders Service", lifespan=lifespan)
FastAPIInstrumentor.instrument_app(app)


class CreateOrderRequest(BaseModel):
    item_id: str = Field(..., description="Item ID")
    quantity: int = Field(default=1, ge=1, description="Quantity")
    amount: float = Field(..., gt=0, description="Total amount")
    customer_id: str = Field(..., description="Customer identifier")


@app.get("/health")
async def health() -> Dict[str, Any]:
    if not db_pool:
        return {"status": "unhealthy", "service": "orders", "db": "not_initialized"}
    try:
        async with db_pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT 1")
        return {"status": "ok", "service": "orders", "db": "connected"}
    except Exception as exc:
        logger.error("Healthcheck DB ping failed: %s", str(exc))
        return {"status": "degraded", "service": "orders", "db_error": str(exc)}


@app.post("/orders", status_code=status.HTTP_201_CREATED)
async def create_order(request: CreateOrderRequest) -> Dict[str, Any]:
    if not db_pool or not http_client:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Service not ready")

    order_id = f"ord_{uuid.uuid4().hex[:12]}"
    logger.info("Creating order %s for customer %s, amount=%.2f",
                order_id, request.customer_id, request.amount)

    # Step 1: Insert pending order into Postgres
    try:
        with tracer.start_as_current_span("db_insert_pending_order"):
            async with db_pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        INSERT INTO orders (id, customer_id, item_id, quantity, amount, status)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        """,
                        (order_id, request.customer_id, request.item_id, request.quantity, request.amount, "pending")
                    )
                    await conn.commit()
    except psycopg_pool.PoolTimeout as exc:
        order_errors_counter.add(1, {"reason": "db_pool_timeout"})
        logger.error("Postgres connection pool exhausted while creating order %s: %s", order_id, str(exc))
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database connection pool exhausted"
        ) from exc
    except Exception as exc:
        order_errors_counter.add(1, {"reason": "db_error"})
        logger.error("Database query error for order %s: %s", order_id, str(exc))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database error"
        ) from exc

    # Step 2: Call Payments service
    payments_timeout_sec = PAYMENTS_TIMEOUT_MS / 1000.0
    try:
        with tracer.start_as_current_span("call_payments_service"):
            logger.info("Calling payments service at %s (timeout=%.3fs)",
                        PAYMENTS_SERVICE_URL, payments_timeout_sec)
            payment_res = await http_client.post(
                f"{PAYMENTS_SERVICE_URL}/pay",
                json={
                    "order_id": order_id,
                    "amount": request.amount,
                    "customer_id": request.customer_id
                },
                timeout=payments_timeout_sec
            )
            if payment_res.status_code >= 400:
                logger.error("Payment failed for order %s: code=%d body=%s",
                             order_id, payment_res.status_code, payment_res.text)
                await _update_order_status(order_id, "failed")
                order_errors_counter.add(1, {"reason": "payment_rejected"})
                raise HTTPException(
                    status_code=payment_res.status_code,
                    detail=f"Payment rejected: {payment_res.text}"
                )
    except httpx.TimeoutException as exc:
        logger.error("Payment timeout for order %s after %.3fs: %s",
                     order_id, payments_timeout_sec, str(exc))
        await _update_order_status(order_id, "failed")
        order_errors_counter.add(1, {"reason": "payment_timeout"})
        raise HTTPException(
            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
            detail="Payment service timed out"
        ) from exc
    except httpx.RequestError as exc:
        logger.error("Payment network error for order %s: %s", order_id, str(exc))
        await _update_order_status(order_id, "failed")
        order_errors_counter.add(1, {"reason": "payment_network_error"})
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Payment service unreachable"
        ) from exc

    # Step 3: Call Inventory service to reserve stock
    try:
        with tracer.start_as_current_span("call_inventory_service"):
            inv_res = await http_client.post(
                f"{INVENTORY_SERVICE_URL}/reserve",
                json={
                    "item_id": request.item_id,
                    "quantity": request.quantity,
                    "order_id": order_id
                },
                timeout=5.0
            )
            if inv_res.status_code >= 400:
                logger.error("Inventory reservation failed for order %s: %s", order_id, inv_res.text)
                await _update_order_status(order_id, "failed")
                order_errors_counter.add(1, {"reason": "inventory_reservation_failed"})
                raise HTTPException(
                    status_code=inv_res.status_code,
                    detail=f"Inventory reservation failed: {inv_res.text}"
                )
    except Exception as exc:
        logger.error("Inventory call error for order %s: %s", order_id, str(exc))
        await _update_order_status(order_id, "failed")
        order_errors_counter.add(1, {"reason": "inventory_error"})
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Inventory service communication error"
        ) from exc

    # Step 4: Mark order confirmed in Postgres
    await _update_order_status(order_id, "confirmed")
    orders_created_counter.add(1, {"status": "confirmed"})
    logger.info("Order %s successfully confirmed", order_id)

    return {
        "order_id": order_id,
        "status": "confirmed",
        "item_id": request.item_id,
        "quantity": request.quantity,
        "amount": request.amount,
        "customer_id": request.customer_id
    }


async def _update_order_status(order_id: str, new_status: str) -> None:
    if not db_pool:
        return
    try:
        async with db_pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "UPDATE orders SET status = %s WHERE id = %s",
                    (new_status, order_id)
                )
                await conn.commit()
    except Exception as exc:
        logger.error("Failed to update status for order %s to %s: %s",
                     order_id, new_status, str(exc))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("testbed.services.orders.main:app", host="0.0.0.0", port=8001, reload=True)
