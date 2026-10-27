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
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;
import org.apache.iceberg.io.AtomicOutputFile;
import org.apache.iceberg.io.CAS;
import org.apache.iceberg.io.InMemoryAtomicFileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.io.SupportsAtomicOperations;

/**
 * An {@link InMemoryAtomicFileIO} whose requests take a store's latency ({@link StoreProfile}),
 * spent in the calling thread, which also counts them ({@link Requests}).
 *
 * <p>A request is: binding an input file (a HEAD: {@code exists} or {@code getLength} of an unbound
 * file), each {@code newStream} (a GET of the whole object), each plain write (a PUT, on close),
 * each conditional write, and each delete. A GET and a conditional write take effect halfway
 * through their latency, as endive's simulator splits them: the request reaches the store, the
 * store reads its state or checks the condition, and the reply comes back. A conditional append
 * that loses its race takes the store's failure latency in all.
 */
final class LatencyFileIO implements SupportsAtomicOperations {

  private final InMemoryAtomicFileIO store;
  private final StoreProfile profile;
  private final String catalogLocation;
  private final double timeScale;
  private final SplittableRandom root;
  private final ThreadLocal<SplittableRandom> random;

  /**
   * {@code store} with {@code profile}'s latencies, scaled by {@code timeScale} (1: as measured),
   * drawn from {@code seed}; the GETs of {@code catalogLocation} count apart.
   */
  LatencyFileIO(
      InMemoryAtomicFileIO store, StoreProfile profile, String catalogLocation, double timeScale,
      long seed) {
    this.store = store;
    this.profile = profile;
    this.catalogLocation = catalogLocation;
    this.timeScale = timeScale;
    this.root = new SplittableRandom(seed);
    this.random = ThreadLocal.withInitial(this::split);
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
    long deadline = System.nanoTime() + nanos;
    for (long left = nanos; left > 0; left = deadline - System.nanoTime()) {
      LockSupport.parkNanos(left);
    }
  }

  @Override
  public boolean supportsAppend() {
    return profile.supportsAppend && store.supportsAppend();
  }

  @Override
  public long maxAppendBytes(String location) {
    return store.maxAppendBytes(location);
  }

  @Override
  public InputFile newInputFile(String location) {
    return new TimedInputFile(store.newInputFile(location));
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

    /** A HEAD, unless this file is bound. */
    private void head() {
      if (!bound) {
        Requests.mine().heads++;
        await(profile.readMs(0, random()));
        bound = true;
      }
    }

    @Override
    public long getLength() {
      head();
      return file.getLength();
    }

    @Override
    public boolean exists() {
      head();
      return file.exists();
    }

    @Override
    public SeekableInputStream newStream() {
      Requests requests = Requests.mine();
      requests.gets++;
      SplittableRandom r = random();
      double half = profile.readMs(0, r) / 2;
      await(half);
      SeekableInputStream stream = file.newStream();
      bound = true;
      long length = file.getLength();
      requests.bytesRead += length;
      if (file.location().equals(catalogLocation)) {
        requests.catalogBytesRead += length;
      }
      await(Math.max(0, profile.readMs(length, r) - half));
      return stream;
    }

    @Override
    public String location() {
      return file.location();
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
