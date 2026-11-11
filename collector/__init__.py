"""A minimal PulseTrace collector.

Ingestion, deduplication, issue grouping, a JSON read API and a small HTML viewer.

It exists to prove the SDK end to end, not to be a production ingestion
platform: it writes synchronously to the database in the request path, where a
real system would buffer through a queue (Kafka, Relay-style edge services).
"""
