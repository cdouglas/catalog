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
import java.io.UncheckedIOException;
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
 * or {@code newStream}) and keeps reading those bytes afterwards. A conditional write expects the
 * version its snapshot has seen ({@link SupportsAtomicOperations#newOutputFile(InputFile)}): one
 * a write returned, or one a lookup ({@code exists}, {@code getLength}) found. A read alone sees
 * none, as on S3, the store whose reads record the least. {@link AtomicOutputFile.Strategy#CAS}
 * replaces the object only if the live generation is the one seen, or, from a snapshot that has
 * seen none, creates it only if it does not exist. {@link AtomicOutputFile.Strategy#APPEND} adds
 * the bytes at the end under the same condition, as S3 Express does with {@code ifMatch} plus
 * {@code writeOffsetBytes}; from a snapshot that has seen no version it looks the object up first,
 * and extends the version it finds if that is still the live one. With {@link
 * #failSupersededReads}, reading a bound generation that a later write replaced fails instead of
 * returning its bytes, as on real storage: GCS reads by generation, and S3 and ADLS with If-Match.
 *
 * <p>Plain {@link OutputFile} writes are unconditional, as for table metadata and data files.
 *
 * <p>{@link #injectFaults} makes conditional writes fail the way remote storage can: before the
 * write lands, or after it lands with the reply lost (an {@link IOException} or a runtime error, as
 * SDKs surface either) or reported as a precondition failure (a transport retry that ran into its
 * own write); or throttled, before or after landing. A lost reply leaves the caller unable to tell
 * whether the write took effect.
 * {@link #injectReadFaults} makes reads fail: a request for an object's metadata or content throws,
 * or its content stream breaks part way through.
 */
public class InMemoryAtomicFileIO implements SupportsAtomicOperations {

  /** How a conditional write fails, if it does. */
  public enum Fault {
    NONE,
    /** Throws {@link IOException}; nothing lands. */
    FAIL_BEFORE,
    /** Lands, then throws {@link IOException}. */
    LOST_REPLY,
    /** Lands, then throws {@link UncheckedIOException}. */
    LOST_REPLY_RUNTIME,
    /** Lands, then throws the strategy's precondition failure, as a retry of it would. */
    LANDED_AS_CONFLICT,
    /** Throws {@link StorageThrottleException} (a 429) before the precondition check; nothing lands. */
    THROTTLED,
    /**
     * Lands, then throws {@link StorageThrottleException}: a 5xx after the service applied the
     * write, or a 429 that ended an SDK's retries of a write that landed.
     */
    LANDED_THROTTLED
  }

  /**
   * Chooses the fault for each conditional write, in order. It is called as the write is issued,
   * before the write reads its precondition, and holds no lock: it may run other writers first.
   */
  @FunctionalInterface
  public interface Faults {
    Fault next(String location, AtomicOutputFile.Strategy strategy);
  }

  /** How a read request fails, if it does. */
  public enum ReadFault {
    NONE,
    /** The request throws {@link UncheckedIOException}; the input file stays unbound. */
    FAIL,
    /**
     * A content request throws {@link IOException} from its stream halfway through the object. A
     * metadata request fails as {@link #FAIL} does.
     */
    FAIL_MID_STREAM
  }

  /**
   * Chooses the fault for each read request, in order. A request is what reaches storage: binding
   * an input file ({@code exists} or {@code getLength} of an unbound file) and every {@code
   * newStream}. It is called as the request is issued and holds no lock: it may run other writers
   * first.
   */
  @FunctionalInterface
  public interface ReadFaults {
    ReadFault next(String location);
  }

  /** One version of an object; a new instance per write. */
  private static final class Version {
    final byte[] bytes;
    final long generation;
    /** Writes the object holds: 1 for the write that made it, plus 1 per append since. */
    final int segments;

    Version(byte[] bytes, long generation, int segments) {
      this.bytes = bytes;
      this.generation = generation;
      this.segments = segments;
    }
  }

  /** A successful conditional write: the version it replaced and the one it wrote. */
  public static final class Write {
    private final String location;
    private final AtomicOutputFile.Strategy strategy;
    private final byte[] before;
    private final byte[] after;

    Write(String location, AtomicOutputFile.Strategy strategy, byte[] before, byte[] after) {
      this.location = location;
      this.strategy = strategy;
      this.before = before;
      this.after = after;
    }

    public String location() {
      return location;
    }

    public AtomicOutputFile.Strategy strategy() {
      return strategy;
    }

    /** Bytes of the replaced version, or null if the write created the object. */
    public byte[] before() {
      return before;
    }

    public byte[] after() {
      return after;
    }
  }

  private final Map<String, Version> objects = new ConcurrentHashMap<>();
  // Successful conditional writes, in the order they took effect.
  private final List<Write> history = Lists.newArrayList();
  private int rejectedAppends = 0;
  private int rejectedCas = 0;
  private final AtomicLong generations = new AtomicLong(0);
  private final boolean supportsAppend;
  private volatile Faults faults = (location, strategy) -> Fault.NONE;
  private final Map<Fault, Integer> injected = Maps.newEnumMap(Fault.class);
  private volatile ReadFaults readFaults = location -> ReadFault.NONE;
  private volatile boolean failSupersededReads = false;
  private volatile int maxSegments = 0;
  private volatile Supplier<RuntimeException> segmentLimitError = null;
  private volatile int appendByteLimit = 0;
  private final Map<ReadFault, Integer> injectedReads = Maps.newEnumMap(ReadFault.class);
  private volatile boolean recordHistory = true;
  private volatile WriteListener listener = null;

  public InMemoryAtomicFileIO() {
    this(true);
  }

  // Java serialization within one JVM (a SerializableTable shipped to a "worker" in a test): the
  // copy is this FileIO itself, its objects included, as InMemoryFileIO's copies share its store.
  // Held weakly: a fuzzer serializes a store per try, and a store no test holds is garbage.
  private static final Map<Long, java.lang.ref.WeakReference<InMemoryAtomicFileIO>> SERIALIZED =
      new ConcurrentHashMap<>();
  private static final AtomicLong SERIAL_IDS = new AtomicLong();
  private final long serialId = SERIAL_IDS.incrementAndGet();

  private Object writeReplace() {
    SERIALIZED.values().removeIf(ref -> ref.get() == null);
    SERIALIZED.put(serialId, new java.lang.ref.WeakReference<>(this));
    return new Serialized(serialId);
  }

  private static final class Serialized implements java.io.Serializable {
    private final long id;

    Serialized(long id) {
      this.id = id;
    }

    private Object readResolve() {
      java.lang.ref.WeakReference<InMemoryAtomicFileIO> ref = SERIALIZED.get(id);
      InMemoryAtomicFileIO io = ref != null ? ref.get() : null;
      if (io == null) {
        throw new IllegalStateException("store " + id + " is gone: no test holds it");
      }
      return io;
    }
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
    if (!(replace instanceof BoundInputFile) || ((BoundInputFile) replace).owner() != this) {
      throw new IllegalArgumentException("Not an input file of this FileIO: " + replace);
    }
    return new ConditionalOutputFile((BoundInputFile) replace);
  }

  @Override
  public synchronized void deleteFile(String location) {
    objects.remove(location);
  }

  /** Fails conditional writes as {@code plan} chooses; {@link Fault#NONE} for all by default. */
  public void injectFaults(Faults plan) {
    this.faults = plan;
  }

  /** Number of faults injected so far, by kind (excluding {@link Fault#NONE}). */
  public synchronized int injected(Fault fault) {
    return injected.getOrDefault(fault, 0);
  }

  /**
   * Whether reading a bound generation that a later write replaced fails ({@link
   * NotFoundException}), as on real storage, instead of returning the replaced bytes.
   */
  public void failSupersededReads(boolean fail) {
    this.failSupersededReads = fail;
  }

  /**
   * Caps the writes an object holds at {@code max}, as S3 Express caps an object at 10,000 parts
   * and an Azure Append Blob at 50,000 blocks: the write that creates or replaces the object is
   * one, and each append adds one. An append past the cap fails, before landing, with {@link
   * SegmentLimitException}, as the providers' FileIOs surface it. 0 removes the cap.
   */
  public void limitSegments(int max) {
    limitSegments(
        max,
        () -> new SegmentLimitException("injected: the object holds " + max + " writes", null));
  }

  /** As {@link #limitSegments(int)}, failing the append with {@code error}. */
  public void limitSegments(int max, Supplier<RuntimeException> error) {
    this.maxSegments = max;
    this.segmentLimitError = error;
  }

  /**
   * Caps the bytes one append writes at {@code max}, and reports the cap ({@link
   * #maxAppendBytes}), as Azure caps an {@code AppendBlock} (4 MiB before service version
   * 2022-11-02, 100 MiB since) and the ADLS FileIO reports it. A larger append fails, before
   * landing, with the raw error the ADLS FileIO lets out (a 413 from the service); a CAS is not
   * capped, since the ADLS FileIO writes one in blocks of the cap. 0 removes the cap.
   */
  public void limitAppendBytes(int max) {
    this.appendByteLimit = max;
  }

  @Override
  public long maxAppendBytes(String location) {
    return appendByteLimit;
  }

  /** Fails read requests as {@code plan} chooses; {@link ReadFault#NONE} for all by default. */
  public void injectReadFaults(ReadFaults plan) {
    this.readFaults = plan;
  }

  /** Number of read faults injected so far, by kind (excluding {@link ReadFault#NONE}). */
  public synchronized int injected(ReadFault fault) {
    return injectedReads.getOrDefault(fault, 0);
  }

  /** The fault for the next read request of {@code location}, recorded if it is one. */
  private ReadFault readFault(String location) {
    ReadFault fault = readFaults.next(location);
    if (fault != ReadFault.NONE) {
      synchronized (this) {
        injectedReads.merge(fault, 1, Integer::sum);
      }
    }
    return fault;
  }

  private static UncheckedIOException readFailure(String location) {
    return new UncheckedIOException(new IOException("injected: read of " + location + " failed"));
  }

  /** Number of objects currently stored. */
  public int size() {
    return objects.size();
  }

  /** Stores {@code bytes} at {@code location} unconditionally, as a new generation. */
  public void seed(String location, byte[] bytes) {
    put(location, bytes.clone());
  }

  /** Number of conditional writes rejected because the object changed, by strategy. */
  public synchronized int rejected(AtomicOutputFile.Strategy strategy) {
    return strategy == AtomicOutputFile.Strategy.APPEND ? rejectedAppends : rejectedCas;
  }

  /** Sees each conditional write that lands, under the store's lock, so in the order they land. */
  public interface WriteListener {
    /** {@code data} is what the write sent: the bytes an append added, or a CAS's whole object. */
    void landed(String location, AtomicOutputFile.Strategy strategy, byte[] data);
  }

  /**
   * Calls {@code seen} with every conditional write that lands from now on (null: none). Unlike
   * {@link #history}, which keeps each version's bytes, it costs only the call: a benchmark that
   * keeps the records a log's writes add uses it.
   */
  public void onWrite(WriteListener seen) {
    this.listener = seen;
  }

  /**
   * Whether conditional writes are kept for {@link #history} (the default). A benchmark that commits
   * thousands of times to a large object turns it off: each kept write holds two versions of it.
   */
  public void recordHistory(boolean record) {
    this.recordHistory = record;
  }

  /** Every successful conditional write, in the order they took effect. */
  public synchronized List<Write> history() {
    return ImmutableList.copyOf(history);
  }

  /** The strategy of every successful conditional write, per location, in commit order. */
  public synchronized Map<String, List<AtomicOutputFile.Strategy>> conditionalWrites() {
    Map<String, List<AtomicOutputFile.Strategy>> writes = Maps.newLinkedHashMap();
    for (Write w : history) {
      writes.computeIfAbsent(w.location, k -> Lists.newArrayList()).add(w.strategy);
    }
    return ImmutableMap.copyOf(writes);
  }

  private synchronized Version put(String location, byte[] bytes) {
    return put(location, bytes, 1);
  }

  private synchronized Version put(String location, byte[] bytes, int segments) {
    Version v = new Version(bytes, generations.incrementAndGet(), segments);
    objects.put(location, v);
    return v;
  }

  private class BoundInputFile implements InputFile {
    private final String location;
    private boolean bound = false;
    private Version version = null;
    // Whether a write returned the bound version or a lookup found it: what a conditional write
    // from this snapshot expects
    private boolean seen = false;

    BoundInputFile(String location) {
      this.location = location;
    }

    BoundInputFile(String location, Version version) {
      this.location = location;
      this.bound = true;
      this.version = version;
      this.seen = true;
    }

    InMemoryAtomicFileIO owner() {
      return InMemoryAtomicFileIO.this;
    }

    /** The bound version, binding it first if need be. Binding is a read request. */
    Version version() {
      synchronized (this) {
        if (bound) {
          return version;
        }
      }
      if (readFault(location) != ReadFault.NONE) {
        throw readFailure(location);
      }
      synchronized (this) {
        if (!bound) {
          bind();
        }
        return version;
      }
    }

    private void bind() {
      version = objects.get(location);
      bound = true;
    }

    /** The bound version, which a lookup has now seen. */
    private Version lookUp() {
      Version v = version();
      synchronized (this) {
        seen = true;
      }
      return v;
    }

    /**
     * The version a conditional write from this snapshot expects (null: no object): the one it has
     * seen; for one that has seen none, no object for a CAS, which assumes the object is absent,
     * and for an APPEND, which extends an existing object, the one a lookup finds now.
     */
    Version expected(AtomicOutputFile.Strategy strategy) {
      synchronized (this) {
        if (seen) {
          return version;
        }
      }
      return strategy == AtomicOutputFile.Strategy.APPEND ? objects.get(location) : null;
    }

    @Override
    public long getLength() {
      Version v = lookUp();
      if (v == null) {
        throw new NotFoundException("File does not exist: %s", location);
      }
      return v.bytes.length;
    }

    @Override
    public SeekableInputStream newStream() {
      ReadFault fault = readFault(location);
      if (fault == ReadFault.FAIL) {
        throw readFailure(location);
      }
      Version v;
      boolean wasBound;
      synchronized (this) {
        wasBound = bound;
        if (!bound) {
          bind();
        }
        v = version;
      }
      if (v == null) {
        throw new NotFoundException("File does not exist: %s", location);
      }
      if (wasBound && failSupersededReads && objects.get(location) != v) {
        throw new NotFoundException("Generation %d of %s was replaced", v.generation, location);
      }
      SeekableInputStream stream = new InMemoryInputFile(location, v.bytes).newStream();
      return fault == ReadFault.FAIL_MID_STREAM
          ? new BreakingInputStream(stream, location, v.bytes.length / 2)
          : stream;
    }

    @Override
    public String location() {
      return location;
    }

    @Override
    public boolean exists() {
      return lookUp() != null;
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
      String location = location();
      Fault fault = faults.next(location, strategy);
      byte[] data = readAll(source.get());
      Version expected = replace.expected(strategy);
      synchronized (InMemoryAtomicFileIO.this) {
        if (fault != Fault.NONE) {
          injected.merge(fault, 1, Integer::sum);
        }
        if (fault == Fault.FAIL_BEFORE) {
          throw new IOException("injected: " + strategy + " at " + location + " failed before landing");
        }
        if (fault == Fault.THROTTLED) {
          String msg = "injected: " + strategy + " at " + location + " throttled";
          throw new StorageThrottleException(msg, new IOException(msg));
        }
        if (strategy == Strategy.APPEND && appendByteLimit > 0 && data.length > appendByteLimit) {
          throw new UncheckedIOException(new IOException(String.format(
              "injected: an append of %d bytes at %s is larger than the store allows (%d)",
              data.length, location, appendByteLimit)));
        }
        Version live = objects.get(location);
        long expectedGen = expected == null ? 0 : expected.generation;
        long liveGen = live == null ? 0 : live.generation;
        if (expectedGen != liveGen) {
          String msg =
              String.format(
                  "%s precondition failed at %s: expected generation %d, live %d",
                  strategy, location, expectedGen, liveGen);
          if (strategy == Strategy.APPEND) {
            rejectedAppends += 1;
            throw new AppendException(msg, new IOException(msg));
          }
          rejectedCas += 1;
          throw new StorageInvariantException(msg, new IOException(msg));
        }
        final byte[] next;
        int segments = strategy == Strategy.APPEND && live != null ? live.segments + 1 : 1;
        if (maxSegments > 0 && segments > maxSegments) {
          throw segmentLimitError.get();
        }
        if (strategy == Strategy.APPEND && live != null) {
          next = new byte[live.bytes.length + data.length];
          System.arraycopy(live.bytes, 0, next, 0, live.bytes.length);
          System.arraycopy(data, 0, next, live.bytes.length, data.length);
        } else {
          next = data;
        }
        if (recordHistory) {
          history.add(new Write(location, strategy, live == null ? null : live.bytes, next));
        }
        BoundInputFile written = new BoundInputFile(location, put(location, next, segments));
        WriteListener seen = listener;
        if (seen != null) {
          seen.landed(location, strategy, data);
        }
        String landed = "injected: " + strategy + " at " + location + " landed, but ";
        switch (fault) {
          case LOST_REPLY:
            throw new IOException(landed + "the reply was lost");
          case LOST_REPLY_RUNTIME:
            throw new UncheckedIOException(new IOException(landed + "the reply was lost"));
          case LANDED_THROTTLED:
            String throttled = landed + "the service replied that it was throttled";
            throw new StorageThrottleException(throttled, new IOException(throttled));
          case LANDED_AS_CONFLICT:
            String msg = landed + "a retry of it failed its precondition";
            if (strategy == Strategy.APPEND) {
              throw new AppendException(msg, new IOException(msg));
            }
            throw new StorageInvariantException(msg, new IOException(msg));
          default:
            return written;
        }
      }
    }
  }

  /** Throws {@link IOException} once a read reaches {@code breakAt}. */
  private static class BreakingInputStream extends SeekableInputStream {
    private final SeekableInputStream in;
    private final String location;
    private final long breakAt;

    BreakingInputStream(SeekableInputStream in, String location, long breakAt) {
      this.in = in;
      this.location = location;
      this.breakAt = breakAt;
    }

    private void check(long pos) throws IOException {
      if (pos >= breakAt) {
        throw new IOException("injected: stream of " + location + " broke at byte " + breakAt);
      }
    }

    @Override
    public long getPos() throws IOException {
      return in.getPos();
    }

    @Override
    public void seek(long newPos) throws IOException {
      in.seek(newPos);
    }

    @Override
    public int read() throws IOException {
      check(in.getPos());
      return in.read();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      long pos = in.getPos();
      check(pos);
      return in.read(b, off, (int) Math.min(len, breakAt - pos));
    }

    @Override
    public void close() throws IOException {
      in.close();
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
