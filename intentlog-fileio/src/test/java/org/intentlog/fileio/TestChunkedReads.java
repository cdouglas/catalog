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
package org.intentlog.fileio;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.io.AtomicOutputFile;
import org.apache.iceberg.io.InMemoryAtomicFileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.io.SupportsAtomicOperations;
import org.intentlog.LogView;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Bug 114 (big ADLS reads fail under appends): reads of an object larger than one download block,
 * as ADLS's SDK stream reads one: a GET per block, each after the first pinned by {@code If-Match}
 * to the version the first found. An append that lands between two blocks fails the next as stale;
 * the read keeps the blocks it read and reads on from where it stopped, in the version live then,
 * and its frames' checks reject a join across a replace.
 */
class TestChunkedReads {
  private static final String LOCATION = "mem:///warehouse/catalog";
  private static final int CHUNK = 4096; // stands for the SDK's 4 MiB block

  private final InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
  private final AtomicInteger gets = new AtomicInteger();
  private final AtomicInteger stale = new AtomicInteger();
  // Runs before every GET of a block after a stream's first: another writer, between two blocks
  private Runnable betweenChunks = () -> { };

  // The check of a tail read against a whole read (the suites set it) would read through the
  // chunked store too: its blocks meet the appends this test makes between blocks, so it is cut and
  // checks nothing, and those appends land after the view the read returned
  private String tailCheck;

  TestChunkedReads() {
    io.setReadsRecordVersion(true);
    io.failSupersededReads(true);
  }

  @BeforeEach
  void noTailCheck() {
    tailCheck = System.clearProperty(AtomicFileLog.CHECK_TAIL_READS);
  }

  @AfterEach
  void restoreTailCheck() {
    if (tailCheck != null) {
      System.setProperty(AtomicFileLog.CHECK_TAIL_READS, tailCheck);
    }
  }

  /** ADLS-like: a stream's GETs are blocks of CHUNK bytes; each after the first If-Match'd. */
  private final class ChunkedIO implements SupportsAtomicOperations {
    @Override
    public InputFile newInputFile(String path) {
      return new ChunkedFile(io.newInputFile(path));
    }

    @Override
    public OutputFile newOutputFile(String path) {
      return io.newOutputFile(path);
    }

    @Override
    public void deleteFile(String path) {
      io.deleteFile(path);
    }

    @Override
    public AtomicOutputFile newOutputFile(InputFile replace) {
      return io.newOutputFile(replace instanceof ChunkedFile c ? c.inner : replace);
    }

    @Override
    public boolean readsRecordVersion() {
      return true;
    }
  }

  private final class ChunkedFile implements InputFile {
    final InputFile inner;

    ChunkedFile(InputFile inner) {
      this.inner = inner;
    }

    @Override
    public long getLength() {
      return inner.getLength();
    }

    @Override
    public SeekableInputStream newStream() {
      return new ChunkedStream(inner);
    }

    @Override
    public String location() {
      return inner.location();
    }

    @Override
    public boolean exists() {
      return inner.exists();
    }
  }

  private final class ChunkedStream extends SeekableInputStream {
    private final InputFile inner;
    private long pos = 0;
    private byte[] chunk = new byte[0];
    private long chunkStart = 0;
    private boolean first = true;
    private long length = -1;

    ChunkedStream(InputFile inner) {
      this.inner = inner;
    }

    /** One GET of [pos, pos + CHUNK): the first binds the version, the rest ask for it. */
    private boolean fetch() throws IOException {
      if (!first) {
        if (pos >= length) {
          return false; // the SDK knows the range's end from the first response
        }
        betweenChunks.run();
      }
      gets.incrementAndGet();
      try (SeekableInputStream s = inner.newStream()) { // a GET of the bound version, if bound
        s.seek(pos);
        byte[] buf = new byte[CHUNK];
        int n = 0;
        while (n < CHUNK) {
          int r = s.read(buf, n, CHUNK - n);
          if (r < 0) {
            break;
          }
          n += r;
        }
        if (first) {
          length = inner.getLength(); // the response's Content-Range: no request
          first = false;
        }
        chunk = Arrays.copyOf(buf, n);
        chunkStart = pos;
        return n > 0;
      } catch (SupportsAtomicOperations.StaleReadException e) {
        stale.incrementAndGet();
        throw e;
      }
    }

    @Override
    public long getPos() {
      return pos;
    }

    @Override
    public void seek(long newPos) {
      pos = newPos;
    }

    @Override
    public int read() throws IOException {
      byte[] one = new byte[1];
      int n = read(one, 0, 1);
      return n < 0 ? -1 : one[0] & 0xff;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      if (pos < chunkStart || pos >= chunkStart + chunk.length) {
        if (!fetch()) {
          return -1;
        }
      }
      int at = (int) (pos - chunkStart);
      int n = Math.min(len, chunk.length - at);
      System.arraycopy(chunk, at, b, off, n);
      pos += n;
      return n;
    }
  }

  private static byte[] random(int length, long seed) {
    byte[] bytes = new byte[length];
    new Random(seed).nextBytes(bytes);
    return bytes;
  }

  private static byte[] bytes(String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }

  private static List<String> strings(LogView view) {
    List<String> out = new ArrayList<>();
    for (byte[] r : view.records()) {
      out.add(new String(r, StandardCharsets.UTF_8));
    }
    return out;
  }

  /** Seeds the object: a checkpoint of {@code checkpointBytes} and a record "r0". */
  private byte[] seedCatalog(int checkpointBytes) {
    byte[] object = AtomicFileLog.encode(random(checkpointBytes, 1), List.of(bytes("r0")));
    io.seed(LOCATION, object);
    return object;
  }

  /** The object as it is now, read whole by a reader whose reads are one GET. */
  private LogView now() {
    return new AtomicFileLog(io, LOCATION).read();
  }

  private static void assertSameView(LogView actual, LogView expected) {
    assertThat(actual.checkpoint().orElseThrow()).isEqualTo(expected.checkpoint().orElseThrow());
    assertThat(actual.records()).containsExactlyElementsOf(expected.records());
    assertThat(actual.tail().bytes()).isEqualTo(expected.tail().bytes());
  }

  /** Another writer's append of {@code record} to the object as it is now. */
  private void appendNow(AtomicFileLog writer, String record) {
    LogView v = writer.read();
    writer.append(bytes(record), v.tail());
  }

  @Test
  void aReadOfOneBlockIsOneGetAndAppendsDoNotMatter() {
    seedCatalog(CHUNK / 2);
    AtomicFileLog writer = new AtomicFileLog(io, LOCATION);
    betweenChunks = () -> appendNow(writer, "w" + gets.get());
    LogView v = new AtomicFileLog(new ChunkedIO(), LOCATION).read();
    assertThat(strings(v)).containsExactly("r0");
    assertThat(gets.get()).isEqualTo(1);
  }

  /**
   * A whole read of a checkpoint fourteen and a half blocks long, with an append between every two
   * blocks: each GET of a block after a stream's first fails as stale, so no read of the version it
   * began on can finish (before the fix, every attempt failed). The read keeps each block it read
   * and reads on from there in the version live then, an extension of the one before, past more
   * cuts than it has attempts, and ends holding the object as it is, with every append that landed
   * while it read.
   */
  @Test
  void aWholeReadAcrossBlocksReadsOnPastAppends() {
    byte[] object = seedCatalog(CHUNK * 29 / 2);
    AtomicFileLog writer = new AtomicFileLog(io, LOCATION);
    AtomicInteger appends = new AtomicInteger();

    // No appends: one whole read, ceil(size / CHUNK) GETs
    LogView quiet = new AtomicFileLog(new ChunkedIO(), LOCATION).read();
    assertThat(gets.get()).isEqualTo((object.length + CHUNK - 1) / CHUNK);
    assertSameView(quiet, now());

    gets.set(0);
    betweenChunks = () -> appendNow(writer, "w" + appends.incrementAndGet());
    LogView busy = new AtomicFileLog(new ChunkedIO(), LOCATION).read();
    betweenChunks = () -> { };
    assertThat(appends.get()).isGreaterThan(AtomicFileLog.READ_ATTEMPTS);
    assertThat(stale.get()).isEqualTo(appends.get());
    assertSameView(busy, now());
    assertThat(strings(busy)).hasSize(1 + appends.get()).startsWith("r0", "w1", "w2");
    // each cut costs the stale GET and nothing else: every block is read once
    assertThat(gets.get()).isEqualTo((object.length + CHUNK - 1) / CHUNK + stale.get());
  }

  /** As above, for a log whose reads look the object up first (GCS, or for ablation). */
  @Test
  void aWholeReadAfterALookupAcrossBlocksReadsOnPastAppends() {
    seedCatalog(CHUNK * 29 / 2);
    AtomicFileLog writer = new AtomicFileLog(io, LOCATION);
    AtomicInteger appends = new AtomicInteger();
    betweenChunks = () -> appendNow(writer, "w" + appends.incrementAndGet());
    LogView busy = new AtomicFileLog(new ChunkedIO(), LOCATION, true).read();
    betweenChunks = () -> { };
    assertThat(stale.get()).isGreaterThan(AtomicFileLog.READ_ATTEMPTS);
    assertSameView(busy, now());
  }

  /**
   * A tail read of many records, fifteen blocks of them, with an append between every two
   * blocks: it reads on from where each block left it, and the view continues the replica it
   * started from (the same checkpoint and records), with every record since.
   */
  @Test
  void aTailReadAcrossBlocksReadsOnPastAppends() {
    seedCatalog(CHUNK / 4);
    AtomicFileLog reader = new AtomicFileLog(new ChunkedIO(), LOCATION);
    LogView held = reader.read();
    AtomicFileLog writer = new AtomicFileLog(io, LOCATION);
    for (int i = 0; i < 60; i++) {
      LogView v = writer.read();
      writer.append(random(CHUNK / 4, 10 + i), v.tail());
    }
    AtomicInteger appends = new AtomicInteger();
    gets.set(0);
    betweenChunks = () -> appendNow(writer, "w" + appends.incrementAndGet());
    LogView after = reader.readAfter(held.tail());
    betweenChunks = () -> { };
    assertThat(stale.get()).isGreaterThan(AtomicFileLog.READ_ATTEMPTS);
    assertSameView(after, now());
    assertThat(after.checkpoint().orElseThrow()).isSameAs(held.checkpoint().orElseThrow());
    assertThat(after.records().get(0)).isSameAs(held.records().get(0));
  }

  /**
   * A read cut part way through the checkpoint by a replace of the object, not an append: the bytes
   * after the cut are another object's, which may even be as long. Its frames' checks reject the
   * join (the checkpoint's checksum, here: the cut is inside the first frame, before any trailer
   * names the incarnation), and the read reads the new object whole.
   */
  @Test
  void aReadCutByAReplaceReadsTheNewObject() {
    seedCatalog(CHUNK * 9 / 2);
    AtomicInteger replaces = new AtomicInteger();
    betweenChunks = () -> {
      if (replaces.getAndIncrement() == 0) {
        // as long as the old object, its checkpoint as long, with other bytes
        io.seed(LOCATION, AtomicFileLog.encode(random(CHUNK * 9 / 2, 2), List.of(bytes("r0"))));
      }
    };
    LogView read = new AtomicFileLog(new ChunkedIO(), LOCATION).read();
    betweenChunks = () -> { };
    assertSameView(read, now());
    assertThat(read.checkpoint().orElseThrow()).isEqualTo(random(CHUNK * 9 / 2, 2));
  }

  /**
   * A read cut by a replace that wrote the same checkpoint again (another incarnation, the same
   * bytes before the cut): what it read before the cut is the new object's too, and the join is
   * the new object, whose checks it passes.
   */
  @Test
  void aReadCutByAReplaceOfTheSameCheckpointReadsTheNewObject() {
    seedCatalog(CHUNK * 9 / 2);
    AtomicInteger replaces = new AtomicInteger();
    betweenChunks = () -> {
      if (replaces.getAndIncrement() == 0) {
        io.seed(LOCATION, AtomicFileLog.encode(random(CHUNK * 9 / 2, 1),
            List.of(bytes("r0"), bytes("r1"))));
      }
    };
    LogView read = new AtomicFileLog(new ChunkedIO(), LOCATION).read();
    betweenChunks = () -> { };
    assertSameView(read, now());
    assertThat(strings(read)).containsExactly("r0", "r1");
  }

  /**
   * A tail read after a compaction: the object it continues was replaced by one whose checkpoint
   * is several blocks long, and writers append between every two blocks. The read finds another
   * object at N - 20, reads it whole, and reads on past each cut.
   */
  @Test
  void aTailReadAfterACompactionReadsTheNewCheckpointPastAppends() {
    seedCatalog(CHUNK / 4);
    AtomicFileLog reader = new AtomicFileLog(new ChunkedIO(), LOCATION);
    LogView held = reader.read();
    AtomicFileLog writer = new AtomicFileLog(io, LOCATION);
    LogView v = writer.read();
    writer.replace(v.tail(), random(CHUNK * 29 / 2, 3), List.of(bytes("c0")));
    AtomicInteger appends = new AtomicInteger();
    betweenChunks = () -> appendNow(writer, "w" + appends.incrementAndGet());
    LogView after = reader.readAfter(held.tail());
    betweenChunks = () -> { };
    assertThat(stale.get()).isGreaterThan(AtomicFileLog.READ_ATTEMPTS);
    assertSameView(after, now());
    assertThat(strings(after)).hasSize(1 + appends.get()).startsWith("c0", "w1");
  }
}
