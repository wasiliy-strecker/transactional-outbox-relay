# Security policy

## Supported versions

This project is currently pre-release. Security fixes are applied to the latest commit on `main`.

## Reporting a vulnerability

Use GitHub's private security advisory flow for this repository. Include:

- affected module and version or commit;
- impact and realistic attack scenario;
- minimal reproduction;
- suggested mitigation when known.

Do not include real credentials, customer payloads, or confidential event data. Please do not open a public issue until a fix and disclosure plan are agreed.

## Scope reminders

The library persists application event payloads in PostgreSQL and transfers them to Kafka. Deployers remain responsible for database and broker authentication, transport encryption, authorization, data classification, retention, secret management, and consumer-side validation.
