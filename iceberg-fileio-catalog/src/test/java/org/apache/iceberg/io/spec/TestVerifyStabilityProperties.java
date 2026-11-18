/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.io.spec;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.statistics.Statistics;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.CatalogMutation;
import org.apache.iceberg.io.CatalogState;
import org.apache.iceberg.io.CatalogTestSupport;
import org.apache.iceberg.io.ProtoCodec;

/**
 * N1: every action's verify is stable (S2). The commit loop reports a commit rejected after one
 * read where its intention does not hold (docs/LOG.md, "Unconditional appends"), and on a log of
 * blind appends a copy of its record may still land after that read: the committed set skips a
 * copy of a record that applied, but not of one that was rejected. That is sound only if an
 * intention that does not hold on a state holds on no state after it.
 *
 * <p>A run grows a history from a small catalog: at each step a writer stages a transaction on
 * a read of the history, the current state or one a few steps back, as {@link CatalogMutation}
 * stages it ({@link CatalogStateGenerators#writerTransaction}), its names binding by the writer's
 * create condition (by name two times in three, by namespace version otherwise, so that writers
 * of both kinds meet), and its record lands on the current state, where
 * replay applies it if its intention verifies. Names come from a small universe, so creates,
 * drops and renames of one name meet often, and a record's versions are those of a read near the
 * current state. Every action that fails its verify where its record lands, and every rejected
 * intention, must fail on every state the history reaches after it; and every state must be
 * well-formed ({@link WellFormed}: names unique within a parent, references intact).
 */
class TestVerifyStabilityProperties {

  /** How far back a writer's read is, in states: mostly the current state or the one before. */
  private static final int[] BACK = {0, 0, 0, 1, 1, 1, 2, 2, 3, 5, 8};

  @Property(tries = 1000)
  void n1_aRejectedActionStaysRejected(
      @ForAll long seed, @ForAll @IntRange(min = 20, max = 150) int steps) {
    new Run(seed).run(steps);
  }

  /** An action or a whole intention that failed its verify where its record landed. */
  private static final class Rejected {
    final int step;
    final ProtoCodec.Action action; // null: the intention
    final CatalogStateGenerators.Staged staged;

    Rejected(int step, ProtoCodec.Action action, CatalogStateGenerators.Staged staged) {
      this.step = step;
      this.action = action;
      this.staged = staged;
    }

    boolean verifies(CatalogState.Builder builder) {
      return action != null ? action.verify(builder) : staged.intention.verify(builder);
    }

    @Override
    public String toString() {
      return (action != null ? action.getClass().getSimpleName() + " of " : "the intention of ")
          + "[" + staged + "] at step " + step;
    }
  }

  private static final class Run {
    private final long seed;
    private final Random rng;
    private final CatalogState.Builder builder = CatalogState.builder();
    private final List<CatalogState> history = new ArrayList<>();
    private final List<Rejected> rejected = new ArrayList<>();
    private final StringBuilder trace = new StringBuilder();
    private int rechecks = 0;
    // Rejected records that bind a name, and how often one found it free again later: the case a
    // check of the name alone would let a late copy apply
    private int freedAgain = 0;

    Run(long seed) {
      this.seed = seed;
      this.rng = new Random(seed);
    }

    void run(int steps) {
      // The catalog's first record names its conflict encoding, as a catalog's creation writes it,
      // or none (then Lamport's)
      if (rng.nextInt(4) != 0) {
        String encoding =
            CatalogTestSupport.ENCODINGS.get(rng.nextInt(CatalogTestSupport.ENCODINGS.size()));
        ProtoCodec.CatalogIntention first = new ProtoCodec.CatalogIntention(
            List.of(new ProtoCodec.SetConflictEncodingAction(encoding)));
        check(first.verify(builder), "the catalog's first record is rejected");
        first.apply(builder);
        trace.append("encoding ").append(encoding).append('\n');
        Statistics.label("encoding").collect(encoding);
      } else {
        Statistics.label("encoding").collect("none");
      }
      // Namespaces and tables to start from: a pointer table and an inline one
      ProtoCodec.CatalogIntention seed = CatalogTestSupport.intention(builder.build(),
          mut -> mut.createNamespace(Namespace.of("a"))
              .createNamespace(Namespace.of("b"))
              .createTable(TableIdentifier.of("a", "t"), "mem:///a/t/seed")
              .createTableInline(TableIdentifier.of("b", "t"),
                  CatalogTestSupport.newInlineTable("mem:///b/t/seed"),
                  CatalogTestSupport.inlineTableCodec()));
      check(seed.verify(builder), "the seed is rejected");
      seed.apply(builder);
      history.add(builder.build());
      int applied = 0;
      for (int step = 1; step <= steps; step++) {
        int back = Math.min(BACK[rng.nextInt(BACK.length)], history.size() - 1);
        CatalogState read = history.get(history.size() - 1 - back);
        // Each writer binds names by its own condition, as writers may differ
        CatalogMutation.CreateCondition condition = rng.nextInt(3) == 0
            ? CatalogMutation.CreateCondition.NAMESPACE : CatalogMutation.CreateCondition.NAME;
        CatalogStateGenerators.Staged staged =
            CatalogStateGenerators.writerTransaction(read, rng, condition);
        if (staged == null) {
          continue;
        }
        if (attempt(step, back, staged)) {
          applied += 1;
        }
      }
      Statistics.label("records applied").collect(Math.min(applied / 10 * 10, 50));
      Statistics.label("rejections rechecked").collect(Math.min(rechecks / 100 * 100, 1000));
      Statistics.label("rejected name bindings rechecked with the name free")
          .collect(freedAgain == 0 ? "0" : freedAgain < 10 ? "1-9" : "10+");
    }

    /** Lands {@code staged}'s record on the current state; whether replay applied it. */
    private boolean attempt(int step, int back, CatalogStateGenerators.Staged staged) {
      List<Rejected> failing = new ArrayList<>();
      for (ProtoCodec.Action action : staged.intention.actions()) {
        if (!action.verify(builder)) {
          failing.add(new Rejected(step, action, staged));
          Statistics.label("actions rejected").collect(action.getClass().getSimpleName());
        }
      }
      boolean applies = staged.intention.verify(builder);
      trace.append(String.format("%d: [%s] read %d back: %s%n",
          step, staged, back, applies ? "applied" : "rejected"));
      if (applies) {
        staged.intention.apply(builder);
        CatalogState state = builder.build();
        history.add(state);
        String violation = WellFormed.violation(state);
        check(violation == null, "step %d left a state that is not well-formed: %s",
            step, violation);
        // A rejection earlier in the history must stay one on this state (S2)
        for (Rejected r : rejected) {
          rechecks += 1;
          check(!r.verifies(builder), "%s verifies at step %d", r, step);
          if (r.action == null && r.staged.aNameIsFree(state)) {
            freedAgain += 1;
          }
        }
      } else {
        rejected.add(new Rejected(step, null, staged));
      }
      rejected.addAll(failing);
      Statistics.label("outcome").collect(applies ? "applied" : "rejected");
      return applies;
    }

    private void check(boolean condition, String format, Object... args) {
      if (!condition) {
        throw new AssertionError(
            String.format(format, args) + " (seed " + seed + ")\ntrace:\n" + trace);
      }
    }
  }
}
