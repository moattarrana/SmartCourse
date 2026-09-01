"""OpenTelemetry tracing setup.

Auto-instruments FastAPI (and httpx if present) and exports spans to an OTLP
collector (Jaeger). Called once at startup from main.py.
"""
import logging

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

logger = logging.getLogger(__name__)


def setup_telemetry(app, service_name: str, otlp_endpoint: str) -> None:
    resource = Resource.create({"service.name": service_name}) # service name 
    provider = TracerProvider(resource=resource) # span provider
    provider.add_span_processor(
        BatchSpanProcessor(OTLPSpanExporter(endpoint=f"{otlp_endpoint}/v1/traces"))  #ships to jaeger
    )
    trace.set_tracer_provider(provider)

    FastAPIInstrumentor.instrument_app(app)  #wraps every route so an incoming request automatically starts a span, times it, and closes it.
    # Instrument outbound httpx calls so trace context propagates downstream.
    try:
        from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor

        HTTPXClientInstrumentor().instrument() #makes traces connected across services,
        #When enrollment-service calls course-service (to check the course is published), 
        # the httpx instrumentation injects W3C trace-context headers into that outbound call.
        #  Course-service's FastAPI instrumentation reads those headers and makes its span a child of the caller's span.
    except Exception as exc:
        logger.info("httpx instrumentation not enabled: %s", exc)

    logger.info("Telemetry enabled for %s -> %s", service_name, otlp_endpoint)
