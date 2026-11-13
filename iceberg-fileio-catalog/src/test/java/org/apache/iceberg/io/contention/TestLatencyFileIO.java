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
package org.apache.iceberg.io.contention;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.apache.iceberg.io.AtomicOutputFile;
import org.apache.iceberg.io.CAS;
import org.apache.iceberg.io.InMemoryAtomicFileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.io.SupportsAtomicOperations;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** The latency model's reads: a GET is priced by the bytes it asks for (TAIL_READS.md). */
class TestLatencyFileIO {
  private static final String CATALOG = "mem:///wh/catalog";
  private static final int SIZE = 3 * 1024 * 1024;

  private InMemoryAtomicFileIO store;
  private LatencyFileIO io;
  private Requests before;

  @BeforeEach
  void before() throws IOException {
    store = new InMemoryAtomicFileIO(true);
    try (PositionOutputStream out = store.newOutputFile(CATALOG).create()) {
      out.write(new byte[SIZE]);
    }
    io = new LatencyFileIO(store, StoreProfile.named("none"), CATALOG, 0, 1);
    before = Requests.mine().copy();
  }

  private Requests requests() {
    return Requests.mine().minus(before);
  }

  private static void readToEnd(SeekableInputStream in) throws IOException {
    byte[] buffer = new byte[8192];
    while (in.read(buffer, 0, buffer.length) >= 0) {
      // drain
    }
  }

  /** Opening a stream binds nothing: a lookup before its first read is a HEAD (errata D23). */
  @Test
  void aLookupAfterAnOpenIsAHeadAndAWholeReadOneGetOfEveryByte() throws IOException {
    InputFile file = io.newInputFile(CATALOG);
    try (SeekableInputStream in = file.newStream()) {
      assertThat(file.getLength()).isEqualTo(SIZE);
      readToEnd(in);
    }

    Requests r = requests();
    assertThat(r.heads).isEqualTo(1);
    assertThat(r.gets).isEqualTo(1);
    assertThat(r.bytesRead).isEqualTo(SIZE);
    assertThat(r.catalogBytesRead).isEqualTo(SIZE);
  }

  /**
   * A lookup takes the profile's HEAD where it measures one (errata D23): azure-2026-11's Get
   * Properties, 5.85 ms at the median, not a GET of nothing, 9.84 ms.
   */
  @Test
  void aLookupTakesTheProfilesHead() {
    LatencyFileIO timed =
        new LatencyFileIO(
            store, StoreProfile.named("azure-2026-11"), CATALOG, 1, 1, new VirtualClock());
    int lookups = 200;
    for (int i = 0; i < lookups; i++) {
      assertThat(timed.newInputFile(CATALOG).getLength()).isEqualTo(SIZE);
    }

    Requests r = requests();
    assertThat(r.heads).isEqualTo(lookups);
    assertThat(r.waitedNanos / 1e6 / lookups).isBetween(5.0, 7.0);
  }

  /**
   * Where the store's reads record their version, a GET binds its file: a lookup after it sends
   * nothing (TAIL_READS.md step 7). Where they do not (GCS), the lookup is a HEAD.
   */
  @Test
  void aGetBindsItsFileWhereTheStoresReadsRecordTheirVersion() throws IOException {
    for (boolean records : new boolean[] {true, false}) {
      store.setReadsRecordVersion(records);
      assertThat(io.readsRecordVersion()).isEqualTo(records);
      Requests start = Requests.mine().copy();
      InputFile file = io.newInputFile(CATALOG);
      try (SeekableInputStream in = file.newStream()) {
        in.seek(SIZE - 100);
        readToEnd(in);
      }
      assertThat(file.getLength()).isEqualTo(SIZE);

      Requests r = Requests.mine().minus(start);
      assertThat(r.gets).isEqualTo(1);
      assertThat(r.bytesRead).isEqualTo(100);
      assertThat(r.heads).as("reads record their version: " + records).isEqualTo(records ? 0 : 1);
    }
  }

  /** A file made with its length sends no lookup, as a client's file given a length does. */
  @Test
  void aFileGivenItsLengthSendsNoLookup() throws IOException {
    InputFile file = io.newInputFile(CATALOG, SIZE);
    assertThat(file.getLength()).isEqualTo(SIZE);
    try (SeekableInputStream in = file.newStream()) {
      readToEnd(in);
    }

    assertThat(requests().heads).isZero();
    assertThat(requests().gets).isEqualTo(1);
  }

  /** A GET from the object's end or past it is a request that reads nothing. */
  @Test
  void aGetFromPastTheEndIsARequestOfNoBytes() throws IOException {
    try (SeekableInputStream in = io.newInputFile(CATALOG).newStream()) {
      in.seek(SIZE + 20);
      assertThat(in.read()).isEqualTo(-1);
    }

    assertThat(requests().gets).isEqualTo(1);
    assertThat(requests().bytesRead).isZero();
  }

  @Test
  void aReadFromASeekIsAGetOfTheBytesFromThere() throws IOException {
    InputFile file = io.newInputFile(CATALOG);
    assertThat(file.getLength()).isEqualTo(SIZE);
    try (SeekableInputStream in = file.newStream()) {
      in.seek(SIZE - 20);
      readToEnd(in);
      assertThat(in.getPos()).isEqualTo(SIZE);
    }

    Requests r = requests();
    assertThat(r.heads).isEqualTo(1);
    assertThat(r.gets).isEqualTo(1);
    assertThat(r.bytesRead).isEqualTo(20);
  }

  @Test
  void aSeekTheGetCannotReadThroughStartsAnother() throws IOException {
    try (SeekableInputStream in = io.newInputFile(CATALOG).newStream()) {
      in.read(new byte[10], 0, 10);
      in.seek(100); // forward, read through
      in.read();
      assertThat(requests().gets).isEqualTo(1);

      in.seek(5); // backward
      in.read();
      assertThat(requests().gets).isEqualTo(2);
      assertThat(requests().bytesRead).isEqualTo(SIZE + (SIZE - 5));

      in.seek(6 + LatencyFileIO.READ_THROUGH + 1); // forward, too far
      in.read();
      assertThat(requests().gets).isEqualTo(3);
    }
  }

  @Test
  void aStreamThatNeverReadsCostsNothing() throws IOException {
    try (SeekableInputStream in = io.newInputFile(CATALOG).newStream()) {
      in.seek(7);
    }

    assertThat(requests().gets).isZero();
    assertThat(requests().bytesRead).isZero();
  }

  /**
   * A clock that spends no time: each wait moves it on, running the actions due by then first, so
   * a test lands a write at a point of a request's latency.
   */
  private static final class VirtualClock implements LatencyFileIO.Clock {
    private final NavigableMap<Long, Runnable> due = new TreeMap<>();
    private long now = 0;

    /** Runs {@code action} {@code ms} from now. */
    void after(double ms, Runnable action) {
      due.put(now + (long) (ms * 1e6), action);
    }

    @Override
    public void await(long nanos) {
      long end = now + nanos;
      for (Map.Entry<Long, Runnable> next = due.pollFirstEntry();
          next != null;
          next = due.pollFirstEntry()) {
        if (next.getKey() > end) {
          due.put(next.getKey(), next.getValue());
          break;
        }
        now = next.getKey();
        next.getValue().run();
      }
      now = end;
    }
  }

  /** The catalog object replaced, as another writer would, by {@code length} bytes. */
  private void replaceWith(int length) {
    try (PositionOutputStream out = store.newOutputFile(CATALOG).createOrOverwrite()) {
      out.write(new byte[length]);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * The store acts on a request halfway through its latency, a read as a conditional write: a
   * lookup sees the object as it is then (errata D24). On the "instant" profile every request takes
   * 1 to about 1.4 ms, so a write 0.2 ms in lands in its first half, and one 0.8 ms in, its second.
   */
  @Test
  void aLookupSeesTheObjectHalfwayThroughItsRequest() {
    VirtualClock clock = new VirtualClock();
    LatencyFileIO timed =
        new LatencyFileIO(store, StoreProfile.named("instant"), CATALOG, 1, 1, clock);
    clock.after(0.2, () -> replaceWith(SIZE + 1));
    clock.after(0.8, () -> replaceWith(SIZE + 2));

    assertThat(timed.newInputFile(CATALOG).getLength()).isEqualTo(SIZE + 1);
  }

  /** A GET serves the version the store holds halfway through the request (errata D24). */
  @Test
  void aGetServesTheVersionHeldHalfwayThroughIt() throws IOException {
    VirtualClock clock = new VirtualClock();
    LatencyFileIO timed =
        new LatencyFileIO(store, StoreProfile.named("instant"), CATALOG, 1, 1, clock);
    InputFile file = timed.newInputFile(CATALOG);
    clock.after(0.2, () -> replaceWith(SIZE + 1));
    clock.after(0.8, () -> replaceWith(SIZE + 2));

    long read = 0;
    try (SeekableInputStream in = file.newStream()) {
      byte[] buffer = new byte[8192];
      for (int n = in.read(buffer); n >= 0; n = in.read(buffer)) {
        read += n;
      }
    }
    assertThat(read).isEqualTo(SIZE + 1);
    assertThat(file.getLength())
        .as("the GET bound the file to the version it read")
        .isEqualTo(SIZE + 1);
  }

  /**
   * A conditional write takes effect halfway through its latency: a write landing in its first
   * half beats it, and one in its second half comes after it.
   */
  @Test
  void aConditionalWriteTakesEffectHalfwayThroughIt() throws IOException {
    VirtualClock clock = new VirtualClock();
    LatencyFileIO timed =
        new LatencyFileIO(store, StoreProfile.named("instant"), CATALOG, 1, 1, clock);
    byte[] record = new byte[16];

    InputFile file = timed.newInputFile(CATALOG);
    assertThat(file.getLength()).isEqualTo(SIZE);
    AtomicOutputFile late = timed.newOutputFile(file);
    CAS token =
        late.prepare(() -> new ByteArrayInputStream(record), AtomicOutputFile.Strategy.APPEND);
    clock.after(0.8, () -> replaceWith(SIZE + 2));
    late.writeAtomic(token, () -> new ByteArrayInputStream(record));
    assertThat(store.newInputFile(CATALOG).getLength())
        .as("replaced after the append")
        .isEqualTo(SIZE + 2);

    InputFile again = timed.newInputFile(CATALOG);
    assertThat(again.getLength()).isEqualTo(SIZE + 2);
    AtomicOutputFile early = timed.newOutputFile(again);
    CAS second =
        early.prepare(() -> new ByteArrayInputStream(record), AtomicOutputFile.Strategy.APPEND);
    clock.after(0.2, () -> replaceWith(SIZE + 3));
    assertThatThrownBy(() -> early.writeAtomic(second, () -> new ByteArrayInputStream(record)))
        .isInstanceOf(SupportsAtomicOperations.AppendException.class);
  }
}
