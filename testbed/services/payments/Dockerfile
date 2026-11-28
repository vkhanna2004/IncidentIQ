FROM python:3.12-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY testbed/ /app/testbed/

EXPOSE 8002

CMD ["uvicorn", "testbed.services.payments.main:app", "--host", "0.0.0.0", "--port", "8002"]
