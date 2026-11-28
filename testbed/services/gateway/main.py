import os
import logging
from contextlib import asynccontextmanager
from typing import Any, Dict

import httpx
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field

from opentelemetry import trace
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor

from testbed.common.telemetry import setup_telemetry

ORDERS_SERVICE_URL = os.getenv("ORDERS_SERVICE_URL", "http://localhost:8001")
INVENTORY_SERVICE_URL = os.getenv("INVENTORY_SERVICE_URL", "http://localhost:8003")

logger = logging.getLogger(__name__)
tracer = trace.get_tracer("gateway")

http_client: httpx.AsyncClient | None = None


class CheckoutRequest(BaseModel):
    item_id: str = Field(..., description="Item ID to purchase")
    quantity: int = Field(default=1, ge=1, description="Quantity")
    amount: float = Field(..., gt=0, description="Total order amount")
    customer_id: str = Field(..., description="Customer ID")


@asynccontextmanager
async def lifespan(app: FastAPI):
    global http_client
    setup_telemetry("gateway")
    HTTPXClientInstrumentor().instrument()
    http_client = httpx.AsyncClient(timeout=10.0)
    logger.info("Gateway service initialized with downstream orders=%s, inventory=%s",
                ORDERS_SERVICE_URL, INVENTORY_SERVICE_URL)
    yield
    if http_client:
        await http_client.aclose()
    logger.info("Gateway service shutdown complete")


app = FastAPI(title="Gateway Service", lifespan=lifespan)
FastAPIInstrumentor.instrument_app(app)


@app.get("/health")
async def health() -> Dict[str, str]:
    return {"status": "ok", "service": "gateway"}


@app.get("/api/products")
async def get_products():
    """Fetch product catalog from inventory service."""
    if not http_client:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Client not ready")

    try:
        response = await http_client.get(f"{INVENTORY_SERVICE_URL}/products")
        if response.status_code != 200:
            logger.error("Failed to fetch products from inventory: %s %s", response.status_code, response.text)
            raise HTTPException(status_code=response.status_code, detail="Failed to fetch products")
        return response.json()
    except httpx.RequestError as exc:
        logger.error("Connection error contacting inventory service: %s", str(exc))
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Inventory service unavailable"
        ) from exc


@app.post("/api/checkout")
async def checkout(request: CheckoutRequest) -> Dict[str, Any]:
    """Public checkout endpoint that delegates order placement to orders service."""
    if not http_client:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Client not ready")

    logger.info("Received checkout request for customer %s, item %s, qty %d",
                request.customer_id, request.item_id, request.quantity)

    with tracer.start_as_current_span("gateway_checkout_span"):
        try:
            payload = {
                "item_id": request.item_id,
                "quantity": request.quantity,
                "amount": request.amount,
                "customer_id": request.customer_id,
            }
            response = await http_client.post(
                f"{ORDERS_SERVICE_URL}/orders",
                json=payload
            )

            if response.status_code >= 400:
                logger.error("Orders service rejected order: status=%d body=%s",
                             response.status_code, response.text)
                raise HTTPException(
                    status_code=response.status_code,
                    detail=f"Order processing failed: {response.text}"
                )

            order_data = response.json()
            logger.info("Order placed successfully: order_id=%s status=%s",
                        order_data.get("order_id"), order_data.get("status"))
            return order_data

        except httpx.TimeoutException as exc:
            logger.error("Timeout waiting for orders service: %s", str(exc))
            raise HTTPException(
                status_code=status.HTTP_504_GATEWAY_TIMEOUT,
                detail="Orders service timed out"
            ) from exc
        except httpx.RequestError as exc:
            logger.error("Network error reaching orders service: %s", str(exc))
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail="Orders service connection failed"
            ) from exc


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("testbed.services.gateway.main:app", host="0.0.0.0", port=8000, reload=True)
