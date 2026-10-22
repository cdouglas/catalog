/**
 * jqwik property-based tests of the protobuf catalog format: codec roundtrip (P1),
 * encode determinism (P2), well-formedness preservation (P4) and builder state
 * surviving the codec (P6) on single states, and {@code TestMultiWriterProperties},
 * which interleaves the commits of several writers on one thread and checks each
 * step of the history. See {@code spec/tla/README.md} for the companion TLA+ model
 * of the commit loop.
 */
package org.apache.iceberg.io.spec;
