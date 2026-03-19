// Integration tests for the queue server.
//
// These tests require a running NATS server, so they are gated behind the
// NATS_URL environment variable.  If it is not set, the tests are skipped.
//
// Run with:
//   NATS_URL=nats://localhost:4222 cargo test -p garage-queue-server
