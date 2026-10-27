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

import java.util.Objects;
import org.apache.iceberg.TableMetadata;

/**
 * One version of an inline table's metadata as the catalog's state holds it: encoded (as a
 * checkpoint or a full update carries it), as {@link TableFields} (as replay keeps it), or both,
 * each made from the other only when asked for and kept. A checkpoint decodes to bytes, and a
 * table no record touches stays bytes; a record that touches one decodes it once, and the versions
 * after are fields until a checkpoint encodes them. A client's load makes the {@link
 * TableMetadata}, kept for the location it was made at. Immutable as a value: each version is a
 * new instance.
 */
final class InlineTable {
  private final byte codec;
  private volatile byte[] bytes;
  private volatile TableFields fields;
  private volatile TableMetadata metadata;

  private InlineTable(byte codec, byte[] bytes, TableFields fields, TableMetadata metadata) {
    this.codec = codec;
    this.bytes = bytes;
    this.fields = fields;
    this.metadata = metadata;
  }

  /** The table encoded as {@code bytes} by codec {@code codec}. */
  static InlineTable ofBytes(byte[] bytes, byte codec) {
    return new InlineTable(codec, Objects.requireNonNull(bytes, "bytes"), null, null);
  }

  /** The table as {@code fields}, to encode with codec {@code codec}. */
  static InlineTable ofFields(TableFields fields, byte codec) {
    return new InlineTable(codec, null, Objects.requireNonNull(fields, "fields"), null);
  }

  /** The table as {@code metadata} (its location ignored), to encode with codec {@code codec}. */
  static InlineTable ofMetadata(TableMetadata metadata, byte codec) {
    return new InlineTable(codec, null, null, Objects.requireNonNull(metadata, "metadata"));
  }

  /** The codec of the table's encoding. */
  byte codec() {
    return codec;
  }

  /** The table encoded, by its codec; encoded once, when first asked for. */
  byte[] bytes() {
    byte[] encoded = bytes;
    if (encoded == null) {
      TableMetadata made = metadata(null);
      encoded = InlineMetadataCodecs.byTag(codec).encode(made);
      if (NativeDelta.CHECK) {
        // The encoding must hold the fields: a replica that decodes it must replay on to what one
        // that kept them does, and encode to the same bytes
        TableMetadata decoded = InlineMetadataCodecs.byTag(codec).decodeFull(encoded, null);
        byte[] again = InlineMetadataCodecs.byTag(codec).encode(decoded);
        String kept = org.apache.iceberg.TableMetadataParser.toJson(made);
        String back = org.apache.iceberg.TableMetadataParser.toJson(decoded);
        if (!java.util.Arrays.equals(encoded, again) || !kept.equals(back)) {
          throw new IllegalStateException(
              "Inline metadata does not round-trip (" + encoded.length + " -> " + again.length
                  + " bytes):\n  kept:    " + kept + "\n  decoded: " + back);
        }
      }
      bytes = encoded;
    }
    return encoded;
  }

  /** The table's fields; decoded once, when first asked for. */
  TableFields fields() {
    TableFields decoded = fields;
    if (decoded == null) {
      TableMetadata made = metadata;
      decoded = TableFields.of(made != null ? made : decode("inline://decode"));
      fields = decoded;
    }
    return decoded;
  }

  /**
   * The table's metadata at {@code location} (null: any). Kept for the location it was last made
   * at; made from the fields, or decoded, otherwise.
   */
  TableMetadata metadata(String location) {
    TableMetadata made = metadata;
    if (made != null && (location == null || location.equals(made.metadataFileLocation()))) {
      return made;
    }
    if (made != null) {
      made = TableFields.of(made).toMetadata(location);
    } else if (fields != null) {
      made = fields.toMetadata(location);
    } else {
      made = decode(location != null ? location : "inline://decode");
    }
    metadata = made;
    return made;
  }

  private TableMetadata decode(String location) {
    return InlineMetadataCodecs.byTag(codec).decodeFull(bytes, location);
  }
}
