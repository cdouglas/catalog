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

import java.util.Map;
import java.util.zip.CRC32;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.util.LocationUtil;

/**
 * Iceberg's file names, which the codecs write as the numbers they are made of rather than as
 * text: a UUID as its 16 bytes, a manifest's name ({@code <commit UUID>-m<N>.avro}, as {@code
 * SnapshotProducer} names one) as its UUID and N, relative to its table's metadata directory.
 * {@link StructuralInlineMetadataCodec} writes manifest lists' and metadata files' names this way,
 * and {@link ProtoCodec} manifests' in the inline tables' pools and in table deltas. A name is
 * written so only if it rebuilds exactly; any other is written whole.
 */
final class IcebergPaths {
  static final int UUID_BYTES = 16;
  private static final String MANIFEST_NUMBER = "-m";
  private static final String AVRO = ".avro";

  private IcebergPaths() {}

  /** The 16 bytes of the UUID {@code s} writes (hex digits and dashes); null if it is not one. */
  static byte[] uuidBytes(String s) {
    if (s == null || s.length() != 36) {
      return null;
    }
    if (s.charAt(8) != '-' || s.charAt(13) != '-' || s.charAt(18) != '-' || s.charAt(23) != '-') {
      return null;
    }
    byte[] out = new byte[UUID_BYTES];
    int p = 0;
    for (int i = 0; i < 36; i++) {
      char c = s.charAt(i);
      if (c == '-') {
        continue;
      }
      int hi = hexValue(c);
      int lo = hexValue(s.charAt(++i));
      if (hi < 0 || lo < 0) {
        return null;
      }
      out[p++] = (byte) ((hi << 4) | lo);
    }
    return out;
  }

  /** The UUID {@code bytes} hold, written as {@code UUID.toString} writes one (lowercase). */
  static String uuid(byte[] bytes) {
    StringBuilder sb = new StringBuilder(36);
    for (int i = 0; i < UUID_BYTES; i++) {
      if (i == 4 || i == 6 || i == 8 || i == 10) {
        sb.append('-');
      }
      sb.append(Character.forDigit((bytes[i] >> 4) & 0xF, 16));
      sb.append(Character.forDigit(bytes[i] & 0xF, 16));
    }
    return sb.toString();
  }

  private static int hexValue(char c) {
    if (c >= '0' && c <= '9') {
      return c - '0';
    }
    if (c >= 'a' && c <= 'f') {
      return c - 'a' + 10;
    }
    if (c >= 'A' && c <= 'F') {
      return c - 'A' + 10;
    }
    return -1;
  }

  /** A manifest's name: the UUID of the commit that wrote it, and its number in that commit. */
  static final class ManifestName {
    final byte[] uuid;
    final long number;

    ManifestName(byte[] uuid, long number) {
      this.uuid = uuid;
      this.number = number;
    }

    @Override
    public String toString() {
      return uuid(uuid) + MANIFEST_NUMBER + number + AVRO;
    }
  }

  /**
   * The name of the manifest at {@code path} if it is {@code directory} followed by a name as
   * SnapshotProducer makes one, which rebuilds exactly; else null.
   */
  static ManifestName manifestName(String directory, String path) {
    if (directory == null || directory.isEmpty() || !path.startsWith(directory)
        || !path.endsWith(AVRO)) {
      return null;
    }
    int start = directory.length();
    int dash = start + 36;
    if (path.length() < dash + MANIFEST_NUMBER.length() + 1 + AVRO.length()
        || !path.startsWith(MANIFEST_NUMBER, dash)) {
      return null;
    }
    byte[] uuid = uuidBytes(path.substring(start, dash));
    String digits = path.substring(dash + MANIFEST_NUMBER.length(), path.length() - AVRO.length());
    if (uuid == null || digits.length() > 18) {
      return null;
    }
    for (int i = 0; i < digits.length(); i++) {
      if (digits.charAt(i) < '0' || digits.charAt(i) > '9') {
        return null;
      }
    }
    ManifestName name = new ManifestName(uuid, Long.parseLong(digits));
    return (directory + name).equals(path) ? name : null;
  }

  /**
   * The directory a table writes its manifests to: its {@code write.metadata.path}, or its
   * location's metadata folder ({@code BaseMetastoreTableOperations.metadataFileLocation}), with
   * a trailing slash. An inline table's manifest prefix: the dictionary its manifests' names are
   * relative to.
   */
  static String metadataDirectory(String location, Map<String, String> properties) {
    String metadataPath = properties.get(TableProperties.WRITE_METADATA_LOCATION);
    if (metadataPath != null) {
      return LocationUtil.stripTrailingSlash(metadataPath) + "/";
    }
    return location + "/metadata/";
  }

  /** The digest a delta states of the prefix its manifests' names are relative to. */
  static int digest(String prefix) {
    CRC32 crc = new CRC32();
    crc.update(prefix.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    return (int) crc.getValue();
  }
}
