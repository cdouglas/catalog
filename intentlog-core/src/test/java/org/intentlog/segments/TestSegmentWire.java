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
package org.intentlog.segments;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.util.List;
import org.intentlog.segments.SegmentFrames.Frame;
import org.intentlog.segments.SegmentFrames.FrameCheckException;
import org.intentlog.wire.InstallSegment;
import org.intentlog.wire.LogEnvelope;
import org.intentlog.wire.RetiredSegment;
import org.junit.jupiter.api.Test;

/** Segment frames, positions and the segment records' wire (docs/LOG_SEGMENTS.md "Records"). */
class TestSegmentWire {

  @Test
  void framesRoundTripRecordsAndSeals() throws Exception {
    byte[] a = new byte[] {1, 2, 3};
    byte[] b = new byte[300];
    b[299] = 7;
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    out.write(SegmentFrames.record(a, 5));
    out.write(SegmentFrames.seal(5));
    out.write(SegmentFrames.record(b, 5));
    byte[] bytes = out.toByteArray();
    List<Frame> frames = SegmentFrames.parse(bytes, bytes.length, 100, 5);
    assertThat(frames).hasSize(3);
    assertThat(frames.get(0).body()).isEqualTo(a);
    assertThat(frames.get(0).offset()).isEqualTo(100);
    assertThat(frames.get(1).seal()).isTrue();
    assertThat(frames.get(1).length()).isEqualTo(SegmentFrames.SEAL);
    assertThat(frames.get(2).body()).isEqualTo(b);
    assertThat(frames.get(2).offset()).isEqualTo(frames.get(1).end());
    assertThat(SegmentFrames.length(b.length)).isEqualTo(frames.get(2).length());
  }

  @Test
  void aFrameOfAnotherSegmentOrCorruptOrCutOffFailsItsCheck() {
    byte[] frame = SegmentFrames.record(new byte[] {9}, 3);
    assertThatThrownBy(() -> SegmentFrames.parse(frame, frame.length, 0, 4))
        .isInstanceOf(FrameCheckException.class).hasMessageContaining("names segment 3");
    byte[] bad = frame.clone();
    bad[1] ^= 1;
    assertThatThrownBy(() -> SegmentFrames.parse(bad, bad.length, 0, 3))
        .isInstanceOf(FrameCheckException.class).hasMessageContaining("fails its check");
    assertThatThrownBy(() -> SegmentFrames.parse(frame, frame.length - 1, 0, 3))
        .isInstanceOf(FrameCheckException.class).hasMessageContaining("cut off");
    assertThatThrownBy(() -> SegmentFrames.record(new byte[0], 3))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void positionsEncodeAndOrderByOrdinalThenRecords() {
    SegmentPosition p = new SegmentPosition(2, 7, 1234);
    assertThat(SegmentPosition.decode(p.encode())).isEqualTo(p);
    assertThat(p.encode()).hasSize(SegmentPosition.BYTES);
    assertThat(new SegmentPosition(3, 0, 0).after(p)).isTrue();
    assertThat(new SegmentPosition(2, 8, 0).after(p)).isTrue();
    assertThat(new SegmentPosition(2, 7, 99).compareTo(p)).isZero();
    assertThat(SegmentPosition.START.encode()).isEqualTo(new byte[24]);
  }

  @Test
  void installAndRetiredRecordsRoundTrip() {
    InstallSegment install = new InstallSegment(4, "seg-00000000000000000004-ab", "", 50_000);
    LogEnvelope.Record r = LogEnvelope.decodeRecord(LogEnvelope.encodeInstallSegment(install));
    assertThat(r.isSegmentRecord()).isTrue();
    assertThat(r.isIntention() || r.isAdvanceHorizon() || r.isPageOut()).isFalse();
    assertThat(r.installSegment()).isEqualTo(install);

    InstallSegment zero = new InstallSegment(0, "", "https://x/y", 1);
    assertThat(LogEnvelope.decodeRecord(LogEnvelope.encodeInstallSegment(zero)).installSegment())
        .isEqualTo(zero);

    RetiredSegment retired = new RetiredSegment(3, "seg-3-cd", "",
        new SegmentPosition(3, 10, 400).encode(), 1_700_000_000_123L);
    LogEnvelope.Record t = LogEnvelope.decodeRecord(LogEnvelope.encodeRetiredSegment(retired));
    assertThat(t.isSegmentRecord()).isTrue();
    assertThat(t.retiredSegment()).isEqualTo(retired);
  }

  @Test
  void aCheckpointPositionRoundTrips() {
    byte[] at = new SegmentPosition(5, 2, 77).encode();
    LogEnvelope.Record r = LogEnvelope.decodeRecord(LogEnvelope.encodeCheckpointThrough(at));
    assertThat(r.isSegmentRecord()).isTrue();
    assertThat(r.checkpointThrough()).isEqualTo(at);
    assertThat(r.installSegment()).isNull();
  }

  /**
   * The segments' prefix is a record of its own (docs/LOG_SEGMENTS.md D13): a tag, a length and
   * the prefix's bytes, so a frame of {@code 2 + n + 21} bytes in the catalog object (its length's
   * varint, then the 20-byte trailer) for a prefix of {@code n < 126} bytes.
   */
  @Test
  void aSegmentPrefixRoundTrips() {
    String prefix = "https://acct.blob.core.windows.net/logs/warehouse/catalog";
    byte[] bytes = LogEnvelope.encodeSegmentPrefix(prefix);
    assertThat(bytes).hasSize(2 + prefix.length());
    LogEnvelope.Record r = LogEnvelope.decodeRecord(bytes);
    assertThat(r.isSegmentRecord()).isTrue();
    assertThat(r.segmentPrefix()).isEqualTo(prefix);
    assertThat(r.checkpointThrough()).isNull();
    assertThat(LogEnvelope.decodeRecord(LogEnvelope.encodeCheckpointThrough(new byte[24]))
        .segmentPrefix()).isNull();
    assertThatThrownBy(() -> LogEnvelope.decodeRecord(new byte[] {0x42, 0}))
        .hasMessageContaining("empty");
  }

  @Test
  void anInstallNeedsALimitAndAName() {
    assertThatThrownBy(() -> new InstallSegment(1, "s", "", 0))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
