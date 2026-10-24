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
package org.intentlog;

import java.io.UncheckedIOException;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * A log in memory, laid out as one object (a checkpoint and the records after it, which a replace
 * rewrites), whose writes can be scripted to fail: the runtime's commit loop, branch by branch.
 * Each version of the log is kept, so a read through a write reads the version it wrote.
 */
final class ScriptedLog implements SharedLog {

  /** What happens to the next write. */
  enum Fault {
    /** The write lands. */
    NONE,
    /** The write lands; its reply is lost. */
    LOST_REPLY,
    /** The write lands; a transport retry of it fails its condition. */
    LANDED_CONFLICT,
    /** Storage throttles the write; nothing lands. */
    THROTTLED,
    /** The write lands; storage then reports a throttle. */
    LANDED_THROTTLED,
    /** The write fails after it was sent, without landing. */
    LOST_REQUEST,
    /** Preparing the write fails; nothing is sent. */
    NOT_SENT
  }

  /** A version of the log: its lineage (bumped by every replace) and contents. */
  private static final class Version {
    final long number;
    final long lineage;
    final byte[] checkpoint;
    final List<byte[]> records;

    Version(long number, long lineage, byte[] checkpoint, List<byte[]> records) {
      this.number = number;
      this.lineage = lineage;
      this.checkpoint = checkpoint;
      this.records = records;
    }

    long bytes() {
      long bytes = checkpoint.length;
      for (byte[] record : records) {
        bytes += record.length;
      }
      return bytes;
    }
  }

  private final class Tail implements LogTail {
    final long number;

    Tail(long number) {
      this.number = number;
    }

    @Override
    public boolean exists() {
      return number > 0;
    }

    @Override
    public long bytes() {
      return versions.get(number).bytes();
    }

    @Override
    public boolean continues(LogTail earlier) {
      return versions.get(number).lineage == versions.get(((Tail) earlier).number).lineage;
    }
  }

  private static final class At implements Receipt {
    final long number;

    At(long number) {
      this.number = number;
    }
  }

  private final Map<Long, Version> versions = new HashMap<>();
  private long live = 0;
  private boolean append = true;
  private int segmentLimit = 0;
  private int segments = 1;
  private long recordLimit = 0;
  private final Deque<Fault> writeFaults = new ArrayDeque<>();
  private int failReads = 0;
  private Runnable beforeWrite = null;
  private Runnable everyWrite = null;
  private Runnable afterWrite = null;
  final List<String> calls = new ArrayList<>();

  ScriptedLog() {
    versions.put(0L, new Version(0, 0, new byte[0], List.of()));
  }

  ScriptedLog withoutAppend() {
    this.append = false;
    return this;
  }

  /** Refuses appends past {@code limit} writes to one lineage (0: no limit). */
  ScriptedLog limitSegments(int limit) {
    this.segmentLimit = limit;
    return this;
  }

  /**
   * Reports {@code limit} as the largest record an append stores, and fails a larger append, before
   * it lands, with an error the log does not classify (0: no limit).
   */
  ScriptedLog limitRecordBytes(long limit) {
    this.recordLimit = limit;
    return this;
  }

  /** The next writes meet these faults, in order. */
  ScriptedLog faults(Fault... faults) {
    writeFaults.addAll(List.of(faults));
    return this;
  }

  /** The next {@code count} reads fail. */
  ScriptedLog failReads(int count) {
    this.failReads = count;
    return this;
  }

  /** Runs {@code race} once, just before the next write: another writer's commit. */
  ScriptedLog beforeNextWrite(Runnable race) {
    this.beforeWrite = race;
    return this;
  }

  /** Runs {@code race} once, just after the next write lands (before its reply). */
  ScriptedLog afterNextWrite(Runnable race) {
    this.afterWrite = race;
    return this;
  }

  /** Runs {@code race} before every write from now on. */
  ScriptedLog beforeEveryWrite(Runnable race) {
    this.everyWrite = race;
    return this;
  }

  /** Appends a record unconditionally, as another writer would: no faults, no hooks. */
  void appendDirectly(byte[] record) {
    Version v = versions.get(live);
    List<byte[]> records = new ArrayList<>(v.records);
    records.add(record);
    install(v.lineage, v.checkpoint, records);
    segments++;
  }

  /** Replaces the log, as another writer's compaction would. */
  void replaceDirectly(byte[] checkpoint, List<byte[]> records) {
    install(versions.get(live).lineage + 1, checkpoint, records);
    segments = 1;
  }

  int versionCount() {
    return versions.size() - 1;
  }

  @Override
  public Capabilities capabilities() {
    return new Capabilities(append, true, true, false, recordLimit);
  }

  @Override
  public LogView read() {
    calls.add("read");
    if (failReads > 0) {
      failReads--;
      throw new UncheckedIOException(new IOException("read failed"));
    }
    return view(live);
  }

  @Override
  public LogView readThrough(Receipt receipt, Duration wait) {
    calls.add("readThrough");
    return view(((At) receipt).number);
  }

  @Override
  public LogTail probe() {
    calls.add("probe");
    return new Tail(live);
  }

  @Override
  public Receipt append(byte[] record, LogTail expected) {
    calls.add("append");
    return write(expected, () -> {
      Version v = versions.get(live);
      if (segmentLimit > 0 && segments + 1 > segmentLimit) {
        throw new LogFullException("full", null);
      }
      if (recordLimit > 0 && record.length > recordLimit) {
        throw new IllegalStateException("a record of " + record.length + " bytes is too large");
      }
      List<byte[]> records = new ArrayList<>(v.records);
      records.add(record);
      segments++;
      return install(v.lineage, v.checkpoint, records);
    });
  }

  @Override
  public Receipt replace(LogTail expected, byte[] checkpoint, List<byte[]> records) {
    calls.add("replace");
    return write(expected, () -> {
      segments = 1;
      return install(versions.get(live).lineage + 1, checkpoint, new ArrayList<>(records));
    });
  }

  private interface Landing {
    long land();
  }

  private Receipt write(LogTail expected, Landing landing) {
    Fault fault = writeFaults.isEmpty() ? Fault.NONE : writeFaults.poll();
    if (fault == Fault.NOT_SENT) {
      throw new LogNotSentException("not sent", null);
    }
    Runnable race = beforeWrite;
    beforeWrite = null;
    if (race != null) {
      race.run();
    }
    if (everyWrite != null) {
      everyWrite.run();
    }
    if (fault == Fault.THROTTLED) {
      throw new LogThrottledException("throttled", null);
    }
    if (fault == Fault.LOST_REQUEST) {
      throw new UncheckedIOException(new IOException("request lost"));
    }
    if (((Tail) expected).number != live) {
      throw new LogConflictException("lost a race", null);
    }
    long written = landing.land();
    Runnable after = afterWrite;
    afterWrite = null;
    if (after != null) {
      after.run();
    }
    switch (fault) {
      case LOST_REPLY:
        throw new UncheckedIOException(new IOException("reply lost"));
      case LANDED_CONFLICT:
        throw new LogConflictException("a retry of a write that landed", null);
      case LANDED_THROTTLED:
        throw new LogThrottledException("throttled after landing", null);
      default:
        return new At(written);
    }
  }

  private long install(long lineage, byte[] checkpoint, List<byte[]> records) {
    long number = versions.size();
    versions.put(number, new Version(number, lineage, checkpoint, List.copyOf(records)));
    live = number;
    return number;
  }

  private LogView view(long number) {
    Version v = versions.get(number);
    Tail tail = new Tail(number);
    return new LogView() {
      @Override
      public LogTail tail() {
        return tail;
      }

      @Override
      public Optional<byte[]> checkpoint() {
        return number == 0 ? Optional.empty() : Optional.of(v.checkpoint);
      }

      @Override
      public List<byte[]> records() {
        return v.records;
      }
    };
  }
}
