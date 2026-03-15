# Test Suite Migration Note

The runtime application is Postgres-only.

Some legacy tests still use SQLite fixtures and should be migrated to Postgres test fixtures.
These legacy tests are not used for production runtime behavior.
