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
package org.apache.iceberg.io;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.iceberg.io.TableScript.Cmd;
import org.apache.iceberg.io.TableScript.Kind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * The same-table conflict matrix of design.md, per conflict encoding: a commit R whose writer read
 * the table just before another commit W landed, under each encoding, against Iceberg's retry
 * ({@link TestRebaseProperties#verify}, which also checks that what an encoding merges is what the
 * retry commits).
 *
 * <p>The table is v2, partitioned by {@code cat} (partitions 1-4), inline TM+ML, with a file in
 * partitions 1 and 2 and a branch {@code b0}. Each row's expected outcomes are one character per
 * encoding, in {@link LocalCatalogMode#ENCODINGS}' order: {@code M} R merged onto W, {@code R} R's
 * record conflicted and Iceberg's retry committed it (a false positive), {@code F} it conflicted
 * and the retry failed (a real conflict), {@code O} it conflicted and is not retried (Iceberg
 * commits it once).
 */
class TestConflictMatrix {

  private static final List<Cmd> PREFIX = List.of(
      cmd(Kind.FAST_APPEND, 0, 0), cmd(Kind.FAST_APPEND, 0, 1), cmd(Kind.CREATE_BRANCH, 0, 1));

  static Stream<Arguments> pairs() {
    return Stream.of(
        row("property change", "append", "RMMMMM",
            cmd(Kind.SET_PROPERTY, 0, 0), cmd(Kind.FAST_APPEND, 0, 2)),
        row("append", "property change", "RRMMMM",
            cmd(Kind.FAST_APPEND, 0, 2), cmd(Kind.SET_PROPERTY, 1, 0)),
        row("sort-order change", "append", "RRMMMM",
            cmd(Kind.SORT_ORDER, 0, 0), cmd(Kind.FAST_APPEND, 0, 2)),
        row("statistics", "append", "RRMMMM",
            cmd(Kind.STATISTICS, 0, 0), cmd(Kind.FAST_APPEND, 0, 2)),
        row("append to a branch", "append to main", "RRMMMM",
            cmd(Kind.BRANCH_APPEND, 0, 2), cmd(Kind.FAST_APPEND, 0, 3)),
        row("append", "append, another partition", "RRRMMM",
            cmd(Kind.FAST_APPEND, 0, 2), cmd(Kind.FAST_APPEND, 0, 3)),
        row("append", "append, same partition", "RRRMMM",
            cmd(Kind.FAST_APPEND, 0, 2), cmd(Kind.FAST_APPEND, 0, 2)),
        row("append", "merge append", "RRRMMM",
            cmd(Kind.FAST_APPEND, 0, 2), cmd(Kind.MERGE_APPEND, 0, 3)),
        row("append", "overwrite, another partition", "RRRRMM",
            cmd(Kind.FAST_APPEND, 0, 2), cmd(Kind.OVERWRITE_PARTITION, 0, 0)),
        row("append", "overwrite, same partition", "FFFFFF",
            cmd(Kind.FAST_APPEND, 0, 0), cmd(Kind.OVERWRITE_PARTITION, 0, 0)),
        row("append", "delete by filter, another partition", "RRRRMM",
            cmd(Kind.FAST_APPEND, 0, 2), cmd(Kind.DELETE_PARTITION, 0, 0)),
        row("append", "compaction, another partition", "RRRRMM",
            cmd(Kind.FAST_APPEND, 0, 2), cmd(Kind.REWRITE_FILES, 0, 0)),
        row("append", "compaction, same partition", "RRRRMM",
            cmd(Kind.FAST_APPEND, 0, 0), cmd(Kind.REWRITE_FILES, 0, 0)),
        // partition-vector stamps the files of every branch, and judges a validated record by
        // them alone: it misses what concern-vector sees, that main did not move
        row("append to a branch", "overwrite, same partition", "RRMMRM",
            cmd(Kind.BRANCH_APPEND, 0, 0), cmd(Kind.OVERWRITE_PARTITION, 0, 0)),
        row("schema change", "append", "RRRMMM",
            cmd(Kind.ADD_COLUMN, 0, 0), cmd(Kind.FAST_APPEND, 0, 2)),
        row("schema change", "overwrite", "RRRRRR",
            cmd(Kind.ADD_COLUMN, 0, 0), cmd(Kind.OVERWRITE_PARTITION, 0, 0)),
        row("partition-spec change", "append", "RRRRRR",
            cmd(Kind.ADD_PARTITION_FIELD, 1, 0), cmd(Kind.FAST_APPEND, 0, 2)),
        row("snapshot expiry", "append", "RRRRRR",
            cmd(Kind.EXPIRE_SNAPSHOTS, 0, 1), cmd(Kind.FAST_APPEND, 0, 2)),
        row("rollback", "append", "RRRRRR",
            cmd(Kind.ROLLBACK, 0, 0), cmd(Kind.FAST_APPEND, 0, 2)),
        row("append", "schema change", "OOOOOO",
            cmd(Kind.FAST_APPEND, 0, 2), cmd(Kind.ADD_COLUMN, 0, 0)));
  }

  @ParameterizedTest(name = "{0} then {1}")
  @MethodSource("pairs")
  void outcomes(String w, String r, String expected, Cmd landed, Cmd stale) {
    List<String> actual = new ArrayList<>();
    for (String encoding : LocalCatalogMode.ENCODINGS) {
      String outcome = TestRebaseProperties.verify(
          encoding, 2, true, LocalCatalogMode.APPEND_ML, PREFIX, List.of(landed), stale)[0];
      actual.add(code(outcome));
    }
    assertThat(String.join("", actual))
        .as("%s then %s, under %s", w, r, LocalCatalogMode.ENCODINGS)
        .isEqualTo(expected);
  }

  private static String code(String outcome) {
    if (outcome.startsWith("admitted")) {
      return "M";
    } else if (outcome.startsWith("conflict, repaired")) {
      return "R";
    } else if (outcome.startsWith("conflict, confirmed")) {
      return "F";
    } else if (outcome.startsWith("conflict, not retried")) {
      return "O";
    }
    return "?(" + outcome + ")";
  }

  private static Arguments row(String w, String r, String expected, Cmd landed, Cmd stale) {
    return Arguments.of(w, r, expected, landed, stale);
  }

  /** A command with operands {@code b} and {@code c} ({@code a} picks transaction sizes only). */
  private static Cmd cmd(Kind kind, int b, int c) {
    return new Cmd(kind, 0, b, c);
  }
}
