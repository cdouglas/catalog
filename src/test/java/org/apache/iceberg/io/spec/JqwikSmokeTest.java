package org.apache.iceberg.io.spec;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;

/** Smoke test proving jqwik is wired into the Maven build. */
class JqwikSmokeTest {

  @Property
  boolean integersAreTheirOwnIdentity(@ForAll int n) {
    return n == n;
  }
}
