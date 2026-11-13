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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.SplittableRandom;
import java.util.function.Supplier;
import org.apache.iceberg.io.AtomicOutputFile;
import org.apache.iceberg.io.CAS;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.InMemoryAtomicFileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.io.SupportsAtomicOperations;
import org.apache.iceberg.io.SupportsPrefixOperations;

/**
 * An {@link InMemoryAtomicFileIO} whose requests take a store's latency ({@link StoreProfile}),
 * spent in the calling thread, which also counts them ({@link Requests}).
 *
 * <p>A request is: a lookup of an unbound input file (a HEAD: {@code exists} or {@code getLength},
 * at the profile's HEAD latency, {@link StoreProfile#headMs}),
 * each GET (below), each plain write (a PUT, on close), each conditional write, each
 * delete, and each listing of a prefix (a LIST, at a read's latency: endive's profiles have none of
 * their own; fileio-catalog lists only to sweep its committed set's orphaned pages). The store acts
 * on a lookup, a GET and a conditional write halfway through its latency, as endive's simulator
 * splits a write: the request reaches the store, the store answers it (a lookup sees the object as
 * it is then, a GET serves the version it holds then, a write's condition is checked and the write
 * takes effect then), and the reply comes back (errata D24). A conditional append that loses its
 * race takes the store's failure latency in all.
 *
 * <p>A stream opened ({@code newStream}) costs nothing and binds nothing until it reads: a GET is a
 * read that starts where no GET is open, of the bytes from there to the end of the version it
 * served, as S3's stream opens a ranged GET at its first read (TAIL_READS.md "Pricing"). A seek
 * the open GET cannot read through (backward, or forward past {@link #READ_THROUGH} bytes, as
 * {@code S3InputStream} skips a short one) ends it, and the next read is a GET from there, of the
 * version the first served. A GET that fails (a version gone, no object) takes a request's round
 * trip, and so does a lookup of no object. So a lookup after an open
 * and before the first read is a HEAD, as on S3 (errata D23). A file is bound by a lookup, by a
 * write that returned it, and by its first GET where the store's reads record their version
 * ({@link #readsRecordVersion}: the profile's, S3's and Azure's, and the store's): then a lookup
 * after the GET sends nothing, as the fork's S3 and ADLS files keep what a GET reported. A file
 * made with its length (a manifest's) is bound from the start, as a client's file given a length
 * sends no lookup.
 */
public final class LatencyFileIO implements SupportsAtomicOperations, SupportsPrefixOperations {
  /** The longest forward seek an open GET reads through, as S3FileIO's default skip size. */
  static final long READ_THROUGH = 1024 * 1024;

  private final InMemoryAtomicFileIO store;
  private final StoreProfile profile;
  private final String catalogLocation;
  private final double timeScale;
  private final SplittableRandom root;
  private final ThreadLocal<SplittableRandom> random;
  private final Clock clock;

  /**
   * How a request's time passes: the calling thread waits through catalog-bench's {@link
   * org.catalogbench.metrics.Waits}, so that how late it woke counts as the driver's delay ({@code
   * late_ms}), as every model wait of the benchmark's does (a test's clock runs instead).
   */
  interface Clock {
    void await(long nanos);
  }

  /**
   * {@code store} with {@code profile}'s latencies, scaled by {@code timeScale} (1: as measured),
   * drawn from {@code seed}; the GETs of {@code catalogLocation} count apart.
   */
  public LatencyFileIO(
      InMemoryAtomicFileIO store, StoreProfile profile, String catalogLocation, double timeScale,
      long seed) {
    this(store, profile, catalogLocation, timeScale, seed, org.catalogbench.metrics.Waits::await);
  }

  LatencyFileIO(
      InMemoryAtomicFileIO store, StoreProfile profile, String catalogLocation, double timeScale,
      long seed, Clock clock) {
    this.store = store;
    this.profile = profile;
    this.catalogLocation = catalogLocation;
    this.timeScale = timeScale;
    this.root = new SplittableRandom(seed);
    this.random = ThreadLocal.withInitial(this::split);
    this.clock = clock;
  }

  private synchronized SplittableRandom split() {
    return root.split();
  }

  private SplittableRandom random() {
    return random.get();
  }

  /** Waits {@code ms} of the store's time, scaled; counts it. */
  private void await(double ms) {
    long nanos = (long) (ms * timeScale * 1e6);
    if (nanos <= 0) {
      return;
    }
    Requests.mine().waitedNanos += nanos;
    clock.await(nanos);
  }

  @Override
  public boolean supportsAppend() {
    return profile.supportsAppend && store.supportsAppend();
  }

  @Override
  public long maxAppendBytes(String location) {
    return store.maxAppendBytes(location);
  }

  /** The profile's and the store's: a GET binds its file only where both say reads record. */
  @Override
  public boolean readsRecordVersion() {
    return profile.readsRecordVersion && store.readsRecordVersion();
  }

  @Override
  public InputFile newInputFile(String location) {
    return new TimedInputFile(store.newInputFile(location));
  }

  /** A file whose length its caller knows (a manifest's): bound, so its lookup sends nothing. */
  @Override
  public InputFile newInputFile(String location, long length) {
    return new TimedInputFile(store.newInputFile(location), true);
  }

  @Override
  public OutputFile newOutputFile(String location) {
    return new TimedOutputFile(store.newOutputFile(location));
  }

  @Override
  public AtomicOutputFile newOutputFile(InputFile replace) {
    InputFile unwrapped = replace instanceof TimedInputFile ? ((TimedInputFile) replace).file : replace;
    return new TimedAtomicOutputFile(store.newOutputFile(unwrapped));
  }

  @Override
  public void deleteFile(String location) {
    Requests.mine().deletes++;
    await(profile.writeMs(0, random()));
    store.deleteFile(location);
  }

  /** One LIST request, at the latency of a read of nothing. */
  @Override
  public Iterable<FileInfo> listPrefix(String prefix) {
    Requests.mine().lists++;
    await(profile.readMs(0, random()));
    return store.listPrefix(prefix);
  }

  /** A listing, then a delete of each object it found. */
  @Override
  public void deletePrefix(String prefix) {
    for (FileInfo file : listPrefix(prefix)) {
      deleteFile(file.location());
    }
  }

  private final class TimedInputFile implements InputFile {
    private final InputFile file;
    private volatile boolean bound;

    TimedInputFile(InputFile file) {
      this(file, false);
    }

    TimedInputFile(InputFile file, boolean bound) {
      this.file = file;
      this.bound = bound;
    }

    /** A HEAD, unless this file is bound: the store answers it halfway through its latency. */
    private <T> T lookup(Supplier<T> answer) {
      if (bound) {
        return answer.get();
      }
      Requests.mine().heads++;
      double ms = profile.headMs(random());
      double half = ms / 2;
      await(half);
      try {
        return answer.get();
      } finally {
        bound = true;
        await(ms - half);
      }
    }

    @Override
    public long getLength() {
      return lookup(file::getLength);
    }

    @Override
    public boolean exists() {
      return lookup(file::exists);
    }

    @Override
    public SeekableInputStream newStream() {
      return new TimedInputStream(this, file.location().equals(catalogLocation));
    }

    @Override
    public String location() {
      return file.location();
    }
  }

  /**
   * A stream of one version, its GETs priced by the bytes each asks for (the class comment): the
   * store's stream, which binds the version it serves, is opened by the first GET, halfway through
   * it.
   */
  private final class TimedInputStream extends SeekableInputStream {
    private final TimedInputFile file;
    private final boolean catalog;
    private SeekableInputStream in = null;
    private long pos = 0;
    private boolean open = false;

    TimedInputStream(TimedInputFile file, boolean catalog) {
      this.file = file;
      this.catalog = catalog;
    }

    /**
     * A GET of the bytes from the position to the end of the version it serves, unless one is
     * open: its latency drawn as a whole read's was (half a request, then the rest of one of its
     * bytes), the version served when the request reaches the store. It binds the file where the
     * store's reads record their version.
     */
    private void get() throws IOException {
      if (open) {
        return;
      }
      open = true;
      Requests requests = Requests.mine();
      requests.gets++;
      SplittableRandom r = random();
      double half = profile.readMs(0, r) / 2;
      await(half);
      long served;
      try {
        if (in == null) {
          in = file.file.newStream(); // the store's GET, in its OPEN mode: binds the version now
          in.seek(pos);
        }
        served = ((InMemoryAtomicFileIO.Served) in).servedLength();
      } catch (IOException | RuntimeException e) {
        await(half);
        throw e;
      }
      if (readsRecordVersion()) {
        file.bound = true;
      }
      long bytes = Math.max(0, served - pos);
      requests.bytesRead += bytes;
      if (catalog) {
        requests.catalogBytesRead += bytes;
      }
      await(Math.max(0, profile.readMs(bytes, r) - half));
    }

    @Override
    public long getPos() {
      return pos;
    }

    @Override
    public void seek(long newPos) throws IOException {
      if (open && (newPos < pos || newPos - pos > READ_THROUGH)) {
        open = false;
      }
      if (in != null) {
        in.seek(newPos);
      }
      pos = newPos;
    }

    @Override
    public int read() throws IOException {
      get();
      int b = in.read();
      if (b >= 0) {
        pos += 1;
      }
      return b;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      if (len == 0) {
        return 0;
      }
      get();
      int n = in.read(b, off, len);
      if (n > 0) {
        pos += n;
      }
      return n;
    }

    @Override
    public int available() throws IOException {
      return in != null ? in.available() : 0;
    }

    @Override
    public void close() throws IOException {
      if (in != null) {
        in.close();
      }
    }
  }

  private final class TimedOutputFile implements OutputFile {
    private final OutputFile file;

    TimedOutputFile(OutputFile file) {
      this.file = file;
    }

    @Override
    public PositionOutputStream create() {
      return new TimedStream(file.create());
    }

    @Override
    public PositionOutputStream createOrOverwrite() {
      return new TimedStream(file.createOrOverwrite());
    }

    @Override
    public String location() {
      return file.location();
    }

    @Override
    public InputFile toInputFile() {
      return new TimedInputFile(file.toInputFile());
    }
  }

  /** A PUT of what is written, when closed. */
  private final class TimedStream extends PositionOutputStream {
    private final PositionOutputStream out;
    private boolean closed = false;

    TimedStream(PositionOutputStream out) {
      this.out = out;
    }

    @Override
    public long getPos() throws IOException {
      return out.getPos();
    }

    @Override
    public void write(int b) throws IOException {
      out.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      out.write(b, off, len);
    }

    @Override
    public void flush() throws IOException {
      out.flush();
    }

    @Override
    public void close() throws IOException {
      if (closed) {
        return;
      }
      closed = true;
      long length = out.getPos();
      Requests requests = Requests.mine();
      requests.puts++;
      requests.bytesWritten += length;
      await(profile.writeMs(length, random()));
      out.close();
    }
  }

  private final class TimedAtomicOutputFile implements AtomicOutputFile {
    private final AtomicOutputFile file;
    private Strategy strategy;

    TimedAtomicOutputFile(AtomicOutputFile file) {
      this.file = file;
    }

    @Override
    public CAS prepare(Supplier<InputStream> source, Strategy howto) throws IOException {
      this.strategy = howto;
      return file.prepare(source, howto);
    }

    @Override
    public InputFile writeAtomic(CAS token, Supplier<InputStream> source) throws IOException {
      byte[] bytes;
      try (InputStream in = source.get()) {
        bytes = in.readAllBytes();
      }
      Requests requests = Requests.mine();
      requests.bytesWritten += bytes.length;
      SplittableRandom r = random();
      boolean append = strategy == Strategy.APPEND;
      double landed = append ? profile.appendMs(r) : profile.casMs(bytes.length, r);
      double half = landed / 2;
      await(half);
      try {
        InputFile written = file.writeAtomic(token, () -> new ByteArrayInputStream(bytes));
        if (append) {
          requests.appendsLanded++;
        } else {
          requests.casLanded++;
        }
        await(landed - half);
        return new TimedInputFile(written, true);
      } catch (SupportsAtomicOperations.StorageThrottleException e) {
        await(landed - half);
        throw e;
      } catch (SupportsAtomicOperations.SegmentLimitException e) {
        // the store refused an append to a full object (its cap): the round trip of a refusal
        await(Math.max(0, profile.appendFailureMs(r) - half));
        throw e;
      } catch (SupportsAtomicOperations.AppendException | SupportsAtomicOperations.CASException e) {
        double lost = append ? profile.appendFailureMs(r) : landed;
        if (append) {
          requests.appendsLost++;
        } else {
          requests.casLost++;
        }
        await(Math.max(0, lost - half));
        throw e;
      } catch (UncheckedIOException e) {
        await(landed - half);
        throw e;
      }
    }

    @Override
    public PositionOutputStream create() {
      return file.create();
    }

    @Override
    public PositionOutputStream createOrOverwrite() {
      return file.createOrOverwrite();
    }

    @Override
    public String location() {
      return file.location();
    }

    @Override
    public InputFile toInputFile() {
      return new TimedInputFile(file.toInputFile());
    }
  }
}
