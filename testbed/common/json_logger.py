
import logging
import json
from datetime import datetime, timezone
from opentelemetry import trace

class JsonFormatter(logging.Formatter):
    def __init__(self, service_name: str, service_version: str = "0.1.0"):
        super().__init__()
        self.service_name = service_name
        self.service_version = service_version

    def format(self, record):
        span = trace.get_current_span()
        span_context = span.get_span_context()

        # If no valid span exists, use empty IDs
        trace_id = ""
        span_id = ""

        if span_context.is_valid:
            trace_id = format(span_context.trace_id, "032x")
            span_id = format(span_context.span_id, "016x")

        log_data = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
            "trace_id": trace_id,
            "span_id": span_id,
            "service.name": self.service_name,
            "service.version": self.service_version
        }

        if record.exc_info:
            log_data["exception"] = self.formatException(
                record.exc_info
            )

        return json.dumps(log_data)


def configure_logging(service_name: str,service_version: str = "0.1.0") -> None:
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    
    # Clear existing handlers to prevent duplicate lines on reload
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
        
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter(service_name, service_version))

    root_logger.addHandler(handler)
