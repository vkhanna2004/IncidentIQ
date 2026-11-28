import os
import uuid
import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Any, Dict

from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field

from opentelemetry import trace
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

from testbed.common.telemetry import setup_telemetry

logger = logging.getLogger(__name__)
tracer = trace.get_tracer("payments")

PAYMENT_PROCESSING_DELAY_MS = float(os.getenv("PAYMENT_PROCESSING_DELAY_MS", "20.0"))
SIMULATE_PAYMENT_FAILURE = os.getenv("SIMULATE_PAYMENT_FAILURE", "false").lower() == "true"


@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_telemetry("payments")
    logger.info("Payments service initialized (delay=%.1fms, simulate_failure=%s)",
                PAYMENT_PROCESSING_DELAY_MS, SIMULATE_PAYMENT_FAILURE)
    yield
    logger.info("Payments service shutdown complete")


app = FastAPI(title="Payments Service", lifespan=lifespan)
FastAPIInstrumentor.instrument_app(app)


class PaymentRequest(BaseModel):
    order_id: str = Field(..., description="Order ID")
    amount: float = Field(..., gt=0, description="Amount to charge")
    customer_id: str = Field(..., description="Customer ID")


@app.get("/health")
async def health() -> Dict[str, str]:
    return {"status": "ok", "service": "payments"}


@app.post("/pay")
async def process_payment(request: PaymentRequest) -> Dict[str, Any]:
    with tracer.start_as_current_span("process_payment_transaction"):
        logger.info("Processing payment for order %s: amount=%.2f, customer=%s",
                    request.order_id, request.amount, request.customer_id)

        # Simulate external payment gateway processing latency
        delay_sec = PAYMENT_PROCESSING_DELAY_MS / 1000.0
        if delay_sec > 0:
            await asyncio.sleep(delay_sec)

        # Simulate payment failure flag
        if SIMULATE_PAYMENT_FAILURE:
            logger.error("Payment gateway declined transaction for order %s: card processor rejected",
                         request.order_id)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Payment processor declined transaction"
            )

        payment_id = f"pay_{uuid.uuid4().hex[:12]}"
        logger.info("Payment succeeded: order_id=%s, payment_id=%s, amount=%.2f",
                    request.order_id, payment_id, request.amount)

        return {
            "status": "success",
            "payment_id": payment_id,
            "order_id": request.order_id,
            "amount": request.amount
        }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("testbed.services.payments.main:app", host="0.0.0.0", port=8002, reload=True)
