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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.intentlog.CommittedSet;
import org.intentlog.LogView;
import org.intentlog.Replay;
import org.intentlog.Replica;
import org.intentlog.RuntimeConfig;
import org.intentlog.fileio.AtomicFileLog;
import org.intentlog.wire.LogEnvelope;

/**
 * Catalog files as bytes, and catalog logs over an {@link InMemoryAtomicFileIO}, for tests here
 * and in {@code org.apache.iceberg.io.spec}: encoding a state to a file, replaying a file
 * ({@link CatalogStateMachine}, which is package-private), and decoding its checkpoint and
 * records.
 */
public final class CatalogTestSupport {

  /**
   * The log id of a file encoded from a state alone ({@link #encodeFullFile(CatalogState)}):
   * fixed, so that the encoding is a function of the state.
   */
  public static final UUID LOG_ID = UUID.fromString("01890000-0000-7000-8000-000000000000");

  private CatalogTestSupport() {}

  /** A catalog file holding only a checkpoint of {@code state}, of log {@link #LOG_ID}. */
  public static byte[] encodeFullFile(CatalogState state) {
    return encodeFullFile(LOG_ID, CommittedSet.EMPTY, state);
  }

  /** A catalog file holding only a checkpoint of {@code replica}: its log id, ids and state. */
  public static byte[] encodeFullFile(Replica<CatalogState> replica) {
    return encodeFullFile(replica.logId(), replica.committed(), replica.state());
  }

  /** A catalog file holding only a checkpoint of {@code state}, with this log id and ids. */
  public static byte[] encodeFullFile(UUID logId, CommittedSet committed, CatalogState state) {
    return AtomicFileLog.encode(checkpoint(logId, committed, state), List.of());
  }

  /** A checkpoint (log.proto LogCheckpoint) of {@code state}, with this log id and ids. */
  public static byte[] checkpoint(UUID logId, CommittedSet committed, CatalogState state) {
    return LogEnvelope.encodeCheckpoint(
        logId, committed.bytes(), CatalogStateMachine.INSTANCE.encodeCheckpoint(state));
  }

  /**
   * The catalog a reader replays from {@code file}: its checkpoint, and the records after it. The
   * replica's tail is the file's length, bound to no object.
   */
  public static Replica<CatalogState> replay(byte[] file) {
    LogView view = AtomicFileLog.decode(file);
    Replay.Result<CatalogState.Builder> result =
        Replay.replay(
            CatalogStateMachine.INSTANCE, view.checkpoint().orElse(null), view.records(), null);
    return new Replica<>(
        result.builder().build(),
        result.committed().build(),
        view.tail(),
        result.intentionRecords(),
        result.logId());
  }

  /** The state a reader replays from {@code file}. */
  public static CatalogState decodeFullFile(byte[] file) {
    return replay(file).state();
  }

  /** The checkpoint of {@code file}, with the committed set and log id the envelope holds. */
  public static LogEnvelope.Checkpoint decodeCheckpointEnvelope(byte[] file) {
    return LogEnvelope.decodeCheckpoint(AtomicFileLog.decode(file).checkpoint().orElseThrow());
  }

  /**
   * The state the checkpoint of {@code file} holds: what replay starts from, before it applies any
   * record.
   */
  public static CatalogState.Builder decodeCheckpoint(byte[] file) {
    return CatalogStateMachine.INSTANCE.decodeCheckpoint(decodeCheckpointEnvelope(file).state());
  }

  /** The checkpoint of {@code file} alone, as a replica: its log id, committed set and state. */
  public static Replica<CatalogState> checkpointOf(byte[] file) {
    LogEnvelope.Checkpoint envelope = decodeCheckpointEnvelope(file);
    return new Replica<>(
        decodeCheckpoint(file).build(),
        CommittedSet.builder().inherit(envelope.committed()).build(),
        null,
        0,
        envelope.logId());
  }

  /** The transaction records of a catalog file, in log order, without its GC records. */
  public static List<ProtoCodec.Transaction> decodeRecords(byte[] file) {
    List<ProtoCodec.Transaction> records = new ArrayList<>();
    for (LogEnvelope.Record record : decodeLogRecords(file)) {
      if (!record.isAdvanceHorizon()) {
        records.add(ProtoCodec.decodeTransaction(record.id(), record.intention()));
      }
    }
    return records;
  }

  /** Every record of a catalog file, GC records included, in log order. */
  public static List<LogEnvelope.Record> decodeLogRecords(byte[] file) {
    List<LogEnvelope.Record> records = new ArrayList<>();
    for (byte[] record : AtomicFileLog.decode(file).records()) {
      records.add(LogEnvelope.decodeRecord(record));
    }
    return records;
  }

  /**
   * {@code state} as a read of log {@link #LOG_ID} with no committed ids would find it: to stage
   * a transaction on ({@link CatalogMutation}) for its intention. It has no tail, so it cannot be
   * committed.
   */
  public static Replica<CatalogState> replicaOf(CatalogState state) {
    return new Replica<>(state, CommittedSet.EMPTY, null, 0, LOG_ID);
  }

  /** The intention of a transaction staged on {@code state}, by {@code change}. */
  public static ProtoCodec.CatalogIntention intention(
      CatalogState state, java.util.function.Consumer<CatalogMutation> change) {
    CatalogMutation mutation = new CatalogMutation(replicaOf(state));
    change.accept(mutation);
    return mutation.intention();
  }

  /** The catalog in the object at {@code location} of {@code io}, committing by the defaults. */
  public static CatalogLog log(SupportsAtomicOperations io, String location) {
    return log(io, location, Map.of());
  }

  /** The catalog in the object at {@code location} of {@code io}, configured by {@code properties}. */
  public static CatalogLog log(
      SupportsAtomicOperations io, String location, Map<String, String> properties) {
    return log(io, location, LogCatalogProperties.runtimeConfig(properties));
  }

  /** The catalog in the object at {@code location} of {@code io}, with these commit knobs. */
  public static CatalogLog log(
      SupportsAtomicOperations io, String location, int maxAppendCount, long maxAppendSize) {
    return log(io, location, maxAppendCount, maxAppendSize,
        LogCatalogProperties.DEFAULT_COMMITTED_RETENTION_MS);
  }

  /** The catalog in the object at {@code location} of {@code io}, with these commit knobs. */
  public static CatalogLog log(
      SupportsAtomicOperations io, String location, int maxAppendCount, long maxAppendSize,
      long committedRetentionMs) {
    return log(io, location, maxAppendCount, maxAppendSize, committedRetentionMs,
        LogCatalogProperties.DEFAULT_THROTTLE_MIN_WAIT_MS);
  }

  /** The catalog in the object at {@code location} of {@code io}, with these commit knobs. */
  public static CatalogLog log(
      SupportsAtomicOperations io, String location, int maxAppendCount, long maxAppendSize,
      long committedRetentionMs, long throttleMinWaitMs) {
    return log(io, location, LogCatalogProperties.runtimeConfig(
        maxAppendCount, maxAppendSize, committedRetentionMs, throttleMinWaitMs));
  }

  /** The catalog in the object at {@code location} of {@code io}, committing by {@code config}. */
  public static CatalogLog log(
      SupportsAtomicOperations io, String location, RuntimeConfig config) {
    return new CatalogLog(new AtomicFileLog(io, location), config);
  }
}
