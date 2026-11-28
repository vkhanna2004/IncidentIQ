import os
import logging
from opentelemetry import trace, metrics
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader

from testbed.common.json_logger import configure_logging

service_version = os.getenv("SERVICE_VERSION", "0.1.0")
otel_exporter_otlp_endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318")


def configure_telemetry(service_name: str):
    resource = Resource.create({
        "service.name": service_name,
        "service.version": service_version
    })

    # Configure tracing
    tracer_provider = TracerProvider(resource=resource)
    span_exporter = OTLPSpanExporter(endpoint=f"{otel_exporter_otlp_endpoint}/v1/traces")
    span_processor = BatchSpanProcessor(span_exporter)
    tracer_provider.add_span_processor(span_processor)
    trace.set_tracer_provider(tracer_provider)

    # Configure metrics
    metric_exporter = OTLPMetricExporter(endpoint=f"{otel_exporter_otlp_endpoint}/v1/metrics")
    metric_reader = PeriodicExportingMetricReader(
        metric_exporter,
        export_interval_millis=5000
    )
    meter_provider = MeterProvider(
        resource=resource,
        metric_readers=[metric_reader]
    )
    metrics.set_meter_provider(meter_provider)

    tracer = trace.get_tracer(service_name)
    meter = metrics.get_meter(service_name)
    return tracer, meter, tracer_provider, meter_provider


def setup_telemetry(service_name: str) -> None:
    """
    Sets up tracing, metrics, and JSON logging for the given service.
    """
    configure_telemetry(service_name)
    configure_logging(service_name, service_version)


if __name__ == "__main__":
    setup_telemetry("test-service")
    logging.info("Testing structured log output")
