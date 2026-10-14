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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.inmemory.InMemoryInputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/**
 * In-memory {@link SupportsAtomicOperations} with the preconditions of real object storage.
 *
 * <p>Every write gives the object a fresh generation, like a GCS generation or an S3 ETag. An
 * {@link InputFile} binds to the generation it first observes ({@code exists}, {@code getLength},
 * or {@code newStream}) and keeps reading those bytes afterwards, so a caller always parses the
 * version its precondition names. {@link AtomicOutputFile.Strategy#CAS} replaces the object only if
 * the live generation is the bound one (or, for an unbound absent file, the object still does not
 * exist). {@link AtomicOutputFile.Strategy#APPEND} adds the bytes at the end under the same
 * condition, as S3 Express does with {@code ifMatch} plus {@code writeOffsetBytes}.
 *
 * <p>Plain {@link OutputFile} writes are unconditional, as for table metadata and data files.
 */
public class InMemoryAtomicFileIO implements SupportsAtomicOperations {

  /** One version of an object; a new instance per write. */
  private static final class Version {
    final byte[] bytes;
    final long generation;

    Version(byte[] bytes, long generation) {
      this.bytes = bytes;
      this.generation = generation;
    }
  }

  private final Map<String, Version> objects = new ConcurrentHashMap<>();
  // Successful conditional writes per location, in the order they took effect.
  private final Map<String, List<AtomicOutputFile.Strategy>> conditionalWrites =
      Maps.newHashMap();
  private final AtomicLong generations = new AtomicLong(0);
  private final boolean supportsAppend;

  public InMemoryAtomicFileIO() {
    this(true);
  }

  /** @param supportsAppend false models a backend without an append primitive (GCS). */
  public InMemoryAtomicFileIO(boolean supportsAppend) {
    this.supportsAppend = supportsAppend;
  }

  @Override
  public boolean supportsAppend() {
    return supportsAppend;
  }

  @Override
  public InputFile newInputFile(String location) {
    return new BoundInputFile(location);
  }

  @Override
  public OutputFile newOutputFile(String location) {
    return new PlainOutputFile(location);
  }

  @Override
  public AtomicOutputFile newOutputFile(InputFile replace) {
    if (!(replace instanceof BoundInputFile)) {
      throw new IllegalArgumentException("Not an input file of this FileIO: " + replace);
    }
    return new ConditionalOutputFile((BoundInputFile) replace);
  }

  @Override
  public synchronized void deleteFile(String location) {
    objects.remove(location);
  }

  /** Number of objects currently stored. */
  public int size() {
    return objects.size();
  }

  /** Stores {@code bytes} at {@code location} unconditionally, as a new generation. */
  public void seed(String location, byte[] bytes) {
    put(location, bytes.clone());
  }

  /** The strategy of every successful conditional write, per location, in commit order. */
  public synchronized Map<String, List<AtomicOutputFile.Strategy>> conditionalWrites() {
    ImmutableMap.Builder<String, List<AtomicOutputFile.Strategy>> copy = ImmutableMap.builder();
    conditionalWrites.forEach((location, writes) -> copy.put(location, ImmutableList.copyOf(writes)));
    return copy.build();
  }

  private synchronized Version put(String location, byte[] bytes) {
    Version v = new Version(bytes, generations.incrementAndGet());
    objects.put(location, v);
    return v;
  }

  private class BoundInputFile implements InputFile {
    private final String location;
    private boolean bound = false;
    private Version version = null;

    BoundInputFile(String location) {
      this.location = location;
    }

    BoundInputFile(String location, Version version) {
      this.location = location;
      this.bound = true;
      this.version = version;
    }

    synchronized Version version() {
      if (!bound) {
        version = objects.get(location);
        bound = true;
      }
      return version;
    }

    @Override
    public long getLength() {
      Version v = version();
      if (v == null) {
        throw new NotFoundException("File does not exist: %s", location);
      }
      return v.bytes.length;
    }

    @Override
    public SeekableInputStream newStream() {
      Version v = version();
      if (v == null) {
        throw new NotFoundException("File does not exist: %s", location);
      }
      return new InMemoryInputFile(location, v.bytes).newStream();
    }

    @Override
    public String location() {
      return location;
    }

    @Override
    public boolean exists() {
      return version() != null;
    }
  }

  private class PlainOutputFile implements OutputFile {
    private final String location;

    PlainOutputFile(String location) {
      this.location = location;
    }

    @Override
    public PositionOutputStream create() {
      if (objects.containsKey(location)) {
        throw new org.apache.iceberg.exceptions.AlreadyExistsException(
            "File already exists: %s", location);
      }
      return createOrOverwrite();
    }

    @Override
    public PositionOutputStream createOrOverwrite() {
      return new BufferingOutputStream(location);
    }

    @Override
    public String location() {
      return location;
    }

    @Override
    public InputFile toInputFile() {
      return newInputFile(location);
    }
  }

  private class ConditionalOutputFile extends PlainOutputFile implements AtomicOutputFile {
    private final BoundInputFile replace;
    private Strategy strategy;

    ConditionalOutputFile(BoundInputFile replace) {
      super(replace.location());
      this.replace = replace;
    }

    @Override
    public CAS prepare(Supplier<InputStream> source, Strategy howto) {
      if (howto == Strategy.APPEND && !supportsAppend) {
        throw new IllegalArgumentException("Append not supported");
      }
      this.strategy = howto;
      return new CAS() {
        @Override
        public long contentLength() {
          return 0;
        }

        @Override
        public byte[] contentChecksumBytes() {
          return new byte[0];
        }

        @Override
        public String contentHeaderString() {
          return "";
        }
      };
    }

    @Override
    public InputFile writeAtomic(CAS token, Supplier<InputStream> source) throws IOException {
      if (strategy == null) {
        throw new IllegalStateException("prepare() was not called");
      }
      byte[] data = readAll(source.get());
      Version expected = replace.version();
      String location = location();
      synchronized (InMemoryAtomicFileIO.this) {
        Version live = objects.get(location);
        long expectedGen = expected == null ? 0 : expected.generation;
        long liveGen = live == null ? 0 : live.generation;
        if (expectedGen != liveGen) {
          String msg =
              String.format(
                  "%s precondition failed at %s: expected generation %d, live %d",
                  strategy, location, expectedGen, liveGen);
          if (strategy == Strategy.APPEND) {
            throw new AppendException(msg, new IOException(msg));
          }
          throw new StorageInvariantException(msg, new IOException(msg));
        }
        final byte[] next;
        if (strategy == Strategy.APPEND && live != null) {
          next = new byte[live.bytes.length + data.length];
          System.arraycopy(live.bytes, 0, next, 0, live.bytes.length);
          System.arraycopy(data, 0, next, live.bytes.length, data.length);
        } else {
          next = data;
        }
        conditionalWrites.computeIfAbsent(location, k -> Lists.newArrayList()).add(strategy);
        return new BoundInputFile(location, put(location, next));
      }
    }
  }

  private class BufferingOutputStream extends PositionOutputStream {
    private final String location;
    private final ByteArrayOutputStream buf = new ByteArrayOutputStream();
    private boolean closed = false;

    BufferingOutputStream(String location) {
      this.location = location;
    }

    @Override
    public long getPos() {
      return buf.size();
    }

    @Override
    public void write(int b) {
      buf.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) {
      buf.write(b, off, len);
    }

    @Override
    public void close() throws IOException {
      if (!closed) {
        closed = true;
        super.close();
        put(location, buf.toByteArray());
      }
    }
  }

  private static byte[] readAll(InputStream in) throws IOException {
    try (InputStream is = in) {
      return is.readAllBytes();
    }
  }
}
