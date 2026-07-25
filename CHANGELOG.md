# Changelog

All notable changes will be documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project intends to follow [Semantic Versioning](https://semver.org/) once its public API stabilizes.

## [Unreleased]

### Added

- Framework-free outbox contracts and lease-aware relay coordinator
- PostgreSQL store with Flyway schema, `SKIP LOCKED` claims, and compare-and-set transitions
- Kafka publisher with reserved metadata headers and failure classification
- Exponential retry backoff, dead-message state, and retention cleanup
- Spring Boot starter with transaction enforcement, type-safe properties, lifecycle, and Micrometer metrics
- Bounded virtual-thread publication using lease-safe claim chunks
- Runnable order-service example with Docker Compose
- PostgreSQL/Kafka Testcontainers coverage and Java 21/25 CI
