/**
 * jqwik property-based tests of single-state invariants of the protobuf catalog
 * format: codec roundtrip (P1), encode determinism (P2), well-formedness
 * preservation (P4), and builder state surviving the codec (P6). They do not
 * interleave concurrent writers. See {@code spec/tla/README.md} for the
 * companion TLA+ model of the commit loop.
 */
package org.apache.iceberg.io.spec;
