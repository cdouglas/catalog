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

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.io.AtomicOutputFile;
import org.apache.iceberg.io.CatalogLog;
import org.apache.iceberg.io.CheckpointRoundTrip;
import org.apache.iceberg.io.InMemoryAtomicFileIO;
import org.intentlog.LogFullException;
import org.intentlog.LogView;
import org.intentlog.RuntimeConfig;
import org.intentlog.fileio.AtomicFileLog;
import org.intentlog.memory.InMemorySharedLog;
import org.intentlog.wire.LogEnvelope;

/**
 * Where {@link TestMultiWriterProperties} keeps the catalog's log, and what it sees of it: the
 * writes that landed, each as the log before and after it in catalog-file bytes (a checkpoint and
 * the records after it), the faults it can inject, copies of records, and the checkpoints to check.
 */
abstract class MultiWriterStorage {

  static final String CATALOG = "mem:///warehouse/catalog";

  /** A fault drawn for a write, served as the store can. */
  enum WriteFault {
    NONE,
    FAIL_BEFORE,
    LOST_REPLY,
    LOST_REPLY_RUNTIME,
    LANDED_AS_CONFLICT,
    THROTTLED,
    LANDED_THROTTLED,
    /** An append that times out in flight and lands later ({@link #deliverDelayed}). */
    DELAYED
  }

  /** A fault drawn for a read. */
  enum ReadFault {
    NONE,
    FAIL,
    FAIL_MID_STREAM,
    /** From an older checkpoint, lagging the tail. */
    STALE
  }

  interface Hooks {
    /** The fault the write of {@code record} meets (null when the store cannot tell). */
    WriteFault beforeWrite(byte[] record);

    ReadFault beforeRead();
  }

  /** A write that landed: the log before and after it, as catalog-file bytes. */
  static final class Write {
    final String kind;
    final byte[] before;
    final byte[] after;
    final byte[] record;

    Write(String kind, byte[] before, byte[] after, byte[] record) {
      this.kind = kind;
      this.before = before;
      this.after = after;
      this.record = record;
    }

    /** A compaction of the log: a replace, whose checkpoint must be a replay of before. */
    boolean replace() {
      return kind.equals("CAS");
    }

    /** A compactor's GC record, which no commit wrote. */
    boolean gc() {
      return kind.equals("GC");
    }
  }

  protected Hooks hooks = null;

  void hook(Hooks newHooks) {
    this.hooks = newHooks;
  }

  abstract String describe();

  abstract CatalogLog log(RuntimeConfig config);

  /** The live log, as catalog-file bytes: what a commit compares with its snapshot. */
  abstract byte[] liveBytes();

  /** How many writes have landed. */
  abstract int writes();

  /** The writes that landed from {@code from} on. */
  abstract List<Write> writes(int from);

  /** How many writes of {@code kind} lost a race (path statistics). */
  abstract int rejected(String kind);

  /** The faults a commit's first and second write meet, by a schedule's operand. */
  abstract List<WriteFault> firstWriteFaults();

  abstract List<WriteFault> secondWriteFaults();

  /** The fault of the failing read a commit's operand picks. */
  abstract ReadFault readFault(int c);

  /** Every intention record that has landed, whatever compaction did to it. */
  abstract List<byte[]> landedRecords();

  /** Appends a copy of {@code record}, unfaulted; false if this store cannot. */
  abstract boolean appendCopy(byte[] record);

  /** A compactor's turn: false if the log compacts only as writers commit. */
  abstract boolean compact();

  /** Lands what was delayed; returns the writes it made. */
  abstract List<Write> deliverDelayed();

  /** Checkpoints stored apart from the log that are not its replay through them. */
  abstract List<String> checkpointViolations();

  /** {@link CheckpointRoundTrip#difference} of a write, or null. */
  String difference(Write write) {
    return CheckpointRoundTrip.difference(write.replace(), write.before, write.after);
  }

  private static boolean isGc(byte[] record) {
    return LogEnvelope.decodeRecord(record).isAdvanceHorizon();
  }

  // ============================================================
  // One object with conditional writes
  // ============================================================

  /** The catalog object of an {@link InMemoryAtomicFileIO}, as before the shared-log split. */
  static final class File extends MultiWriterStorage {
    // A commit's first write fails two times in three, its second one time in three.
    private static final List<WriteFault> FIRST = List.of(
        WriteFault.NONE, WriteFault.NONE, WriteFault.NONE, WriteFault.NONE,
        WriteFault.FAIL_BEFORE,
        WriteFault.LOST_REPLY, WriteFault.LOST_REPLY,
        WriteFault.LOST_REPLY_RUNTIME,
        WriteFault.LANDED_AS_CONFLICT, WriteFault.LANDED_AS_CONFLICT,
        WriteFault.THROTTLED, WriteFault.LANDED_THROTTLED);
    private static final List<WriteFault> SECOND = List.of(
        WriteFault.NONE, WriteFault.NONE, WriteFault.NONE, WriteFault.NONE,
        WriteFault.NONE, WriteFault.NONE, WriteFault.NONE,
        WriteFault.FAIL_BEFORE, WriteFault.LOST_REPLY, WriteFault.LOST_REPLY_RUNTIME,
        WriteFault.LANDED_AS_CONFLICT, WriteFault.LANDED_THROTTLED);

    private final InMemoryAtomicFileIO io = new InMemoryAtomicFileIO();
    private final String described;

    File(boolean strictVersions, int segmentLimit) {
      this.described = "one object" + (strictVersions ? ", strict versions" : "")
          + (segmentLimit > 0 ? ", " + segmentLimit + " writes at most" : "");
      if (segmentLimit > 0) {
        io.limitSegments(segmentLimit);
      }
      io.failSupersededReads(strictVersions);
      io.injectFaults((location, strategy) -> hooks == null
          ? InMemoryAtomicFileIO.Fault.NONE
          : InMemoryAtomicFileIO.Fault.valueOf(hooks.beforeWrite(null).name()));
      io.injectReadFaults(location -> hooks == null
          ? InMemoryAtomicFileIO.ReadFault.NONE
          : InMemoryAtomicFileIO.ReadFault.valueOf(hooks.beforeRead().name()));
    }

    @Override
    String describe() {
      return described;
    }

    @Override
    CatalogLog log(RuntimeConfig config) {
      return new CatalogLog(new AtomicFileLog(io, CATALOG), config);
    }

    @Override
    byte[] liveBytes() {
      try (InputStream in = io.newInputFile(CATALOG).newStream()) {
        return in.readAllBytes();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    @Override
    int writes() {
      return io.history().size();
    }

    @Override
    List<Write> writes(int from) {
      List<InMemoryAtomicFileIO.Write> history = io.history();
      List<Write> writes = new ArrayList<>();
      for (InMemoryAtomicFileIO.Write w : history.subList(from, history.size())) {
        writes.add(new Write(w.strategy().name(), w.before(), w.after(), null));
      }
      return writes;
    }

    @Override
    int rejected(String kind) {
      return io.rejected(AtomicOutputFile.Strategy.valueOf(kind));
    }

    @Override
    List<WriteFault> firstWriteFaults() {
      return FIRST;
    }

    @Override
    List<WriteFault> secondWriteFaults() {
      return SECOND;
    }

    @Override
    ReadFault readFault(int c) {
      return c % 2 == 0 ? ReadFault.FAIL : ReadFault.FAIL_MID_STREAM;
    }

    @Override
    List<byte[]> landedRecords() {
      Set<ByteBuffer> seen = new LinkedHashSet<>();
      for (InMemoryAtomicFileIO.Write w : io.history()) {
        for (byte[] record : AtomicFileLog.decode(w.after()).records()) {
          if (!isGc(record)) {
            seen.add(ByteBuffer.wrap(record));
          }
        }
      }
      List<byte[]> records = new ArrayList<>();
      seen.forEach(b -> records.add(b.array()));
      return records;
    }

    @Override
    boolean appendCopy(byte[] record) {
      Hooks was = hooks;
      hooks = null;
      try {
        AtomicFileLog log = new AtomicFileLog(io, CATALOG);
        if (io.supportsAppend()) {
          try {
            log.append(record, log.probe());
            return true;
          } catch (LogFullException e) {
            // the object holds as many writes as the store allows: copy it with the record added
          }
        }
        LogView view = log.read();
        List<byte[]> records = new ArrayList<>(view.records());
        records.add(record);
        log.replace(view.tail(), view.checkpoint().orElseThrow(), records);
        return true;
      } finally {
        hooks = was;
      }
    }

    @Override
    boolean compact() {
      return false;
    }

    @Override
    List<Write> deliverDelayed() {
      return List.of();
    }

    @Override
    List<String> checkpointViolations() {
      return List.of(); // each CAS write is checked as it is made
    }
  }

  // ============================================================
  // A shared log with checkpoints apart
  // ============================================================

  /**
   * An {@link InMemorySharedLog}. Each append that landed is a write, from the log through the
   * record before it to the log through it, in catalog-file bytes: its first checkpoint and every
   * record since, which the log keeps. Compactors run with the writers' retention, a second ahead.
   */
  static final class Memory extends MultiWriterStorage {
    private static final List<WriteFault> FIRST = List.of(
        WriteFault.NONE, WriteFault.NONE, WriteFault.NONE, WriteFault.NONE,
        WriteFault.FAIL_BEFORE,
        WriteFault.LOST_REPLY, WriteFault.LOST_REPLY,
        WriteFault.DELAYED, WriteFault.DELAYED,
        WriteFault.LANDED_AS_CONFLICT,
        WriteFault.THROTTLED, WriteFault.LANDED_THROTTLED);
    private static final List<WriteFault> SECOND = List.of(
        WriteFault.NONE, WriteFault.NONE, WriteFault.NONE, WriteFault.NONE,
        WriteFault.NONE, WriteFault.NONE, WriteFault.NONE,
        WriteFault.FAIL_BEFORE, WriteFault.LOST_REPLY, WriteFault.DELAYED,
        WriteFault.LANDED_AS_CONFLICT, WriteFault.LANDED_THROTTLED);

    private final InMemorySharedLog log;
    private CatalogLog compactor = null;

    Memory(boolean conditional) {
      this.log = new InMemorySharedLog(CATALOG, conditional);
      log.intercept(new InMemorySharedLog.Interceptor() {
        @Override
        public InMemorySharedLog.WriteFault beforeWrite(
            InMemorySharedLog.Request request, byte[] payload) {
          if (hooks == null || request != InMemorySharedLog.Request.APPEND) {
            return InMemorySharedLog.WriteFault.NONE;
          }
          WriteFault fault = hooks.beforeWrite(payload);
          return InMemorySharedLog.WriteFault.valueOf(
              fault == WriteFault.LOST_REPLY_RUNTIME ? "LOST_REPLY" : fault.name());
        }

        @Override
        public InMemorySharedLog.ReadShape beforeRead(InMemorySharedLog.Request request) {
          if (hooks == null) {
            return InMemorySharedLog.ReadShape.NORMAL;
          }
          switch (hooks.beforeRead()) {
            case FAIL:
            case FAIL_MID_STREAM:
              return InMemorySharedLog.ReadShape.FAIL;
            case STALE:
              return InMemorySharedLog.ReadShape.of(1, 2);
            default:
              return InMemorySharedLog.ReadShape.NORMAL;
          }
        }
      });
    }

    @Override
    String describe() {
      return "a shared log" + (log.capabilities().conditionalAppend() ? ", conditional" : "");
    }

    @Override
    CatalogLog log(RuntimeConfig config) {
      return new CatalogLog(log, config);
    }

    /** Compactions run with {@code config}, from the first call on. */
    void compactWith(RuntimeConfig config) {
      this.compactor = new CatalogLog(log, config);
    }

    private byte[] first() {
      return log.checkpoints().get(0L);
    }

    @Override
    byte[] liveBytes() {
      return AtomicFileLog.encode(first(), log.records());
    }

    private List<InMemorySharedLog.Write> appends() {
      List<InMemorySharedLog.Write> appends = new ArrayList<>();
      for (InMemorySharedLog.Write w : log.history()) {
        if (w.request() == InMemorySharedLog.Request.APPEND) {
          appends.add(w);
        }
      }
      return appends;
    }

    @Override
    int writes() {
      return appends().size();
    }

    @Override
    List<Write> writes(int from) {
      List<InMemorySharedLog.Write> appends = appends();
      List<byte[]> records = log.records();
      List<Write> writes = new ArrayList<>();
      for (InMemorySharedLog.Write w : appends.subList(from, appends.size())) {
        int at = (int) w.position();
        writes.add(new Write(isGc(w.payload()) ? "GC" : "APPEND",
            AtomicFileLog.encode(first(), records.subList(0, at - 1)),
            AtomicFileLog.encode(first(), records.subList(0, at)), w.payload()));
      }
      return writes;
    }

    @Override
    int rejected(String kind) {
      return 0;
    }

    @Override
    List<WriteFault> firstWriteFaults() {
      return FIRST;
    }

    @Override
    List<WriteFault> secondWriteFaults() {
      return SECOND;
    }

    @Override
    ReadFault readFault(int c) {
      return c % 2 == 0 ? ReadFault.FAIL : ReadFault.STALE;
    }

    @Override
    List<byte[]> landedRecords() {
      List<byte[]> intentions = new ArrayList<>();
      for (byte[] record : log.records()) {
        if (!isGc(record)) {
          intentions.add(record);
        }
      }
      return intentions;
    }

    @Override
    boolean appendCopy(byte[] record) {
      Hooks was = hooks;
      hooks = null;
      try {
        log.append(record, null);
        return true;
      } finally {
        hooks = was;
      }
    }

    @Override
    boolean compact() {
      Hooks was = hooks;
      hooks = null;
      try {
        compactor.compact();
        return true;
      } finally {
        hooks = was;
      }
    }

    @Override
    List<Write> deliverDelayed() {
      int before = writes();
      log.deliverDelayed();
      return writes(before);
    }

    @Override
    List<String> checkpointViolations() {
      List<String> violations = new ArrayList<>(log.violations());
      List<byte[]> records = log.records();
      for (Map.Entry<Long, byte[]> c : log.checkpoints().entrySet()) {
        if (c.getKey() == 0L) {
          continue;
        }
        String difference = CheckpointRoundTrip.checkpointIsReplay(
            AtomicFileLog.encode(first(), records.subList(0, c.getKey().intValue())),
            AtomicFileLog.encode(c.getValue(), List.of()));
        if (difference != null) {
          violations.add("the checkpoint at position " + c.getKey() + ": " + difference);
        }
      }
      return violations;
    }
  }
}
