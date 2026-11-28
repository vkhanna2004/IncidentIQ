import pytest
from fastapi.testclient import TestClient

from testbed.services.gateway.main import app as gateway_app
from testbed.services.payments.main import app as payments_app


def test_gateway_health():
    with TestClient(gateway_app) as client:
        res = client.get("/health")
        assert res.status_code == 200
        assert res.json() == {"status": "ok", "service": "gateway"}


def test_payments_health_and_pay():
    with TestClient(payments_app) as client:
        health_res = client.get("/health")
        assert health_res.status_code == 200
        assert health_res.json() == {"status": "ok", "service": "payments"}

        pay_res = client.post("/pay", json={
            "order_id": "ord_test123",
            "amount": 49.99,
            "customer_id": "cust_1"
        })
        assert pay_res.status_code == 200
        data = pay_res.json()
        assert data["status"] == "success"
        assert data["order_id"] == "ord_test123"
        assert "payment_id" in data
