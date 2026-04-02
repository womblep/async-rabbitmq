# Changelog

All notable changes to this project will be documented in this file.

## [0.1.1] - 2026-04-02

### Added

- Expose `passive:` keyword argument on `Channel#queue` and `Channel#exchange` for defensive
  startup checks that assert a resource exists without creating it (AMQP 0-9-1 passive declare).
- Integration tests for passive declare (exists and 404 paths) for both queues and exchanges.
