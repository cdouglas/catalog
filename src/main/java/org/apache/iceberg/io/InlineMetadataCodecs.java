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

/**
 * Registry of {@link InlineMetadataCodec} instances and helpers for resolving
 * a codec from its on-wire tag or short name.
 */
public final class InlineMetadataCodecs {

  /** Proto tag for {@code CODEC_JSON_GZIP}. */
  public static final byte TAG_JSON_GZIP = 0;

  /** Proto tag for {@code CODEC_STRUCTURAL}. */
  public static final byte TAG_STRUCTURAL = 1;

  public static final InlineMetadataCodec JSON_GZIP = new GzipInlineMetadataCodec();
  public static final InlineMetadataCodec STRUCTURAL = new StructuralInlineMetadataCodec();

  private InlineMetadataCodecs() {}

  /** Returns the codec instance for the on-wire {@link InlineMetadataCodec#tag()}. */
  public static InlineMetadataCodec byTag(int tag) {
    switch (tag) {
      case TAG_JSON_GZIP:
        return JSON_GZIP;
      case TAG_STRUCTURAL:
        return STRUCTURAL;
      default:
        throw new IllegalArgumentException(
            "Unknown InlineMetadataCodec tag: " + tag
                + " (expected " + TAG_JSON_GZIP + " or " + TAG_STRUCTURAL + ")");
    }
  }

  /** Returns the codec instance for the {@code fileio.catalog.inline.tm.codec} config value. */
  public static InlineMetadataCodec byShortName(String shortName) {
    if (shortName == null) {
      throw new IllegalArgumentException("InlineMetadataCodec shortName cannot be null");
    }
    switch (shortName) {
      case "gzip":
      case "json-gzip":
        return JSON_GZIP;
      case "structural":
        return STRUCTURAL;
      default:
        throw new IllegalArgumentException(
            "Unknown InlineMetadataCodec shortName: '" + shortName
                + "' (expected 'gzip' or 'structural')");
    }
  }
}
