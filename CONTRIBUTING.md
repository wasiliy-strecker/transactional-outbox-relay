# Contributing

## Development environment

- JDK 21 or 25
- Maven Wrapper included in the repository
- Docker-compatible runtime for integration tests

Build everything that does not require infrastructure:

```bash
./mvnw verify
```

Run the PostgreSQL and Kafka suites:

```bash
./mvnw -Pintegration verify
```

Apply Java formatting before committing:

```bash
./mvnw spotless:apply
```

## Change guidelines

- Keep `outbox-core` independent of frameworks and infrastructure clients.
- Preserve defensive copies for payloads and headers.
- Document delivery guarantees conservatively.
- Add a focused unit test for behavior changes.
- Add or extend a Testcontainers test for SQL, transaction, or broker behavior.
- Keep claims, retries, cleanup, payloads, and concurrency bounded.
- Never log event payloads or secrets.
- Update architecture or operations documentation when semantics change.

## Commit and pull request scope

Use imperative, focused commit subjects. A pull request should explain:

- the problem and intended behavior;
- delivery or compatibility impact;
- verification commands and results;
- migration or operational changes;
- known limitations.

Do not combine unrelated refactoring with a semantic change.

## Public API changes

The project is pre-`1.0.0`, but public contracts still require care. Explain source and behavior compatibility, update examples, and add a changelog entry.

## Reporting security issues

Do not open a public issue for a suspected vulnerability. Follow [SECURITY.md](SECURITY.md).
