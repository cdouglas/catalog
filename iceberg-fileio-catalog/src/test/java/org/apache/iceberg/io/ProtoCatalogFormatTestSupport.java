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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.intentlog.wire.LogEnvelope;

/**
 * Test-only bridge that exposes the full-file encode/decode cycle for
 * property-based tests in {@code org.apache.iceberg.io.spec}.
 *
 * <p>Lives in {@code org.apache.iceberg.io} so it can access the
 * package-private {@link ProtoCatalogFormat#readInternal} and the
 * package-private {@link ProtoCatalogFormat.Mut} constructor.
 */
public final class ProtoCatalogFormatTestSupport {

  private ProtoCatalogFormatTestSupport() {}

  /**
   * Encodes a {@link ProtoCatalogFile} to full LCAT wire bytes:
   * {@code [MAGIC][version][varint-len][checkpoint-bytes]}.
   *
   * <p>No appended transaction records are included; the checkpoint already
   * contains the complete catalog state.
   */
  public static byte[] encodeFullFile(ProtoCatalogFile catalog) {
    try {
      ByteArrayOutputStream out = new ByteArrayOutputStream();

      // Header: 4-byte magic + 4-byte big-endian format version.
      out.write(ProtoCatalogFormat.MAGIC);
      writeBigEndianInt(out, ProtoCatalogFormat.FORMAT_VERSION);

      // Checkpoint: encode the catalog state using the standard ProtoCodec path.
      ProtoCatalogFormat.ProtoIdManager idManager = new ProtoCatalogFormat.ProtoIdManager();
      idManager.setGlobals(catalog.nextNamespaceId(), catalog.nextTableId());
      // Mut with no pending mutations; we only need it as a CatalogFile.Mut reference.
      ProtoCatalogFormat.Mut mut = new ProtoCatalogFormat.Mut(catalog);
      byte[] checkpoint = ProtoCodec.encodeCheckpoint(catalog, mut, idManager);

      writeVarint(out, checkpoint.length);
      out.write(checkpoint);

      return out.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Decodes a full LCAT file (as produced by {@link #encodeFullFile}) back to a
   * {@link ProtoCatalogFile}, replaying any appended transaction records.
   *
   * <p>The returned file is bound to a stub {@link InputFile} at location
   * {@code "test://decoded"}.
   */
  public static ProtoCatalogFile decodeFullFile(byte[] bytes) {
    // Stub InputFile: readInternal stores it as the location reference on the
    // resulting ProtoCatalogFile. The location string is not used during decode.
    InputFile location = new StubInputFile("test://decoded");
    try {
      return ProtoCatalogFormat.readInternal(location, new ByteArrayInputStream(bytes), bytes.length);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * The state the checkpoint of a full LCAT file holds: what replay starts from, before it applies
   * any record.
   */
  public static ProtoCatalogFile.Builder decodeCheckpoint(byte[] bytes) {
    int[] at = {ProtoCatalogFormat.HEADER_SIZE};
    int length = readVarint(bytes, at);
    ProtoCatalogFile.Builder builder = ProtoCatalogFile.builder(new StubInputFile("test://decoded"));
    ProtoCodec.decodeCheckpoint(Arrays.copyOfRange(bytes, at[0], at[0] + length), builder);
    return builder;
  }

  /** The transaction records of a full LCAT file, in log order, without its GC records. */
  public static List<ProtoCodec.Transaction> decodeRecords(byte[] bytes) {
    List<ProtoCodec.Transaction> records = new ArrayList<>();
    for (LogEnvelope.Record record : decodeLogRecords(bytes)) {
      if (!record.isAdvanceHorizon()) {
        records.add(ProtoCodec.decodeTransaction(record.id(), record.intention()));
      }
    }
    return records;
  }

  /** Every record of a full LCAT file, GC records included, in log order. */
  public static List<LogEnvelope.Record> decodeLogRecords(byte[] bytes) {
    int[] at = {ProtoCatalogFormat.HEADER_SIZE};
    int checkpointLength = readVarint(bytes, at);
    at[0] += checkpointLength;
    List<LogEnvelope.Record> records = new ArrayList<>();
    while (at[0] < bytes.length) {
      int length = readVarint(bytes, at);
      records.add(LogEnvelope.decodeRecord(Arrays.copyOfRange(bytes, at[0], at[0] + length)));
      at[0] += length;
    }
    return records;
  }

  // -----------------------------------------------------------------------
  // Private helpers
  // -----------------------------------------------------------------------

  /** Reads the varint at {@code at[0]} and advances {@code at[0]} past it. */
  private static int readVarint(byte[] bytes, int[] at) {
    int value = 0;
    for (int shift = 0; ; shift += 7) {
      byte b = bytes[at[0]++];
      value |= (b & 0x7F) << shift;
      if (b >= 0) {
        return value;
      }
    }
  }

  private static void writeBigEndianInt(ByteArrayOutputStream out, int v) {
    out.write((v >>> 24) & 0xFF);
    out.write((v >>> 16) & 0xFF);
    out.write((v >>> 8) & 0xFF);
    out.write(v & 0xFF);
  }

  private static void writeVarint(ByteArrayOutputStream out, int v) {
    while ((v & ~0x7F) != 0) {
      out.write((v & 0x7F) | 0x80);
      v >>>= 7;
    }
    out.write(v);
  }

  private static final class StubInputFile implements InputFile {
    private final String location;

    StubInputFile(String location) {
      this.location = location;
    }

    @Override public long getLength() { return 0; }
    @Override public SeekableInputStream newStream() {
      throw new UnsupportedOperationException("StubInputFile.newStream");
    }
    @Override public String location() { return location; }
    @Override public boolean exists() { return false; }
  }
}
