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

import java.io.IOException;
import org.apache.iceberg.io.InMemoryAtomicFileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SeekableInputStream;
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
}
