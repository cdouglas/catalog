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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.intentlog.fileio.AtomicFileLog;

/**
 * {@code src/main/proto/catalog.proto}, the schema of record for the catalog's checkpoints and
 * intentions, with intentlog-core's {@code log.proto}, the schema of the log envelope that carries
 * them, and a check that catalog file bytes conform to the two. The codecs are written by hand ({@link ProtoCodec},
 * {@link InlineDeltaCodec}), so nothing else ties them to the schema; the format explorer decodes
 * files with it, and a field it lacks shows as unknown there.
 *
 * <p>{@link #violation} walks a whole file: the header, the checkpoint and each log record, into
 * every nested message. A file conforms if every field is declared in its message, with a type
 * of the wire type it was written with, a declared enum value, a singular field or oneof member at
 * most once, and valid UTF-8 in strings. {@link CheckpointRoundTrip} applies it to every catalog
 * file the suites and fuzzers write.
 */
final class CatalogProto {
  static final Path PROTO_FILE = Paths.get("src/main/proto/catalog.proto");
  static final Path LOG_PROTO_FILE = Paths.get("../intentlog-core/src/main/proto/log.proto");

  /** The envelope's payload fields (bytes in log.proto), and the catalog message each carries. */
  static final Map<String, String> PAYLOADS =
      Map.of("LogCheckpoint.state", "Checkpoint", "LogRecord.intention", "Intention");

  private static final int WIRE_VARINT = 0;
  private static final int WIRE_FIXED64 = 1;
  private static final int WIRE_LENGTH_DELIMITED = 2;
  private static final int WIRE_FIXED32 = 5;

  private static final Map<String, Integer> SCALAR_WIRE_TYPES = new LinkedHashMap<>();

  static {
    for (String type :
        List.of("int32", "int64", "uint32", "uint64", "sint32", "sint64", "bool")) {
      SCALAR_WIRE_TYPES.put(type, WIRE_VARINT);
    }
    for (String type : List.of("fixed64", "sfixed64", "double")) {
      SCALAR_WIRE_TYPES.put(type, WIRE_FIXED64);
    }
    for (String type : List.of("fixed32", "sfixed32", "float")) {
      SCALAR_WIRE_TYPES.put(type, WIRE_FIXED32);
    }
    SCALAR_WIRE_TYPES.put("string", WIRE_LENGTH_DELIMITED);
    SCALAR_WIRE_TYPES.put("bytes", WIRE_LENGTH_DELIMITED);
  }

  private static Schema schema;

  private CatalogProto() {}

  /** log.proto and catalog.proto, parsed once. */
  static synchronized Schema schema() {
    if (schema == null) {
      try {
        schema = parse(Files.readString(LOG_PROTO_FILE), Files.readString(PROTO_FILE));
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
    return schema;
  }

  /** Where {@code catalogFile} departs from catalog.proto, or null if it conforms. */
  static String violation(byte[] catalogFile) {
    return violation(schema(), catalogFile);
  }

  /** Where {@code catalogFile} departs from {@code schema}, or null if it conforms. */
  static String violation(Schema schema, byte[] catalogFile) {
    try {
      new Walk(schema, catalogFile).file();
      return null;
    } catch (Violation e) {
      return e.getMessage();
    }
  }

  // ============================================================
  // Schema
  // ============================================================

  static final class Schema {
    final Map<String, Message> messages;
    final Map<String, EnumDef> enums;

    Schema(Map<String, Message> messages, Map<String, EnumDef> enums) {
      this.messages = messages;
      this.enums = enums;
    }
  }

  static final class Message {
    final String name;
    final List<Field> fields = new ArrayList<>();
    final Set<Integer> reserved = new HashSet<>();

    Message(String name) {
      this.name = name;
    }

    Field field(int number) {
      for (Field field : fields) {
        if (field.number == number) {
          return field;
        }
      }
      return null;
    }
  }

  static final class Field {
    final int number;
    final String name;
    final String type;
    final boolean repeated;
    final boolean packed;
    final String oneof;
    final String comment;

    Field(
        int number,
        String name,
        String type,
        boolean repeated,
        boolean packed,
        String oneof,
        String comment) {
      this.number = number;
      this.name = name;
      this.type = type;
      this.repeated = repeated;
      this.packed = packed;
      this.oneof = oneof;
      this.comment = comment;
    }
  }

  static final class EnumDef {
    final String name;
    final Map<String, Integer> values = new LinkedHashMap<>();

    EnumDef(String name) {
      this.name = name;
    }
  }

  private static final Pattern MESSAGE_START = Pattern.compile("^\\s*message\\s+(\\w+)\\s*\\{");
  private static final Pattern ENUM_START = Pattern.compile("^\\s*enum\\s+(\\w+)\\s*\\{");
  private static final Pattern ONEOF_START = Pattern.compile("^\\s*oneof\\s+(\\w+)\\s*\\{");
  private static final Pattern FIELD =
      Pattern.compile(
          "^\\s*(repeated\\s+)?([\\w.]+)\\s+(\\w+)\\s*=\\s*(\\d+)\\s*(\\[([^\\]]*)\\])?\\s*;");
  private static final Pattern MAP_FIELD =
      Pattern.compile(
          "^\\s*map<\\s*([\\w.]+)\\s*,\\s*([\\w.]+)\\s*>\\s+(\\w+)\\s*=\\s*(\\d+)\\s*;");
  private static final Pattern RESERVED = Pattern.compile("^\\s*reserved\\s+([\\d\\s,]+);");
  private static final Pattern ENUM_VALUE = Pattern.compile("^\\s*(\\w+)\\s*=\\s*(-?\\d+)\\s*;");
  private static final Pattern CLOSE = Pattern.compile("^\\s*\\}");

  /**
   * log.proto and catalog.proto as one schema, the envelope's payload fields ({@link #PAYLOADS})
   * typed as the catalog messages they carry: a payload is length-delimited either way, and a
   * protobuf reader decodes it with the message the field names.
   */
  static Schema parse(String logSource, String catalogSource) {
    Schema log = parse(logSource);
    Schema catalog = parse(catalogSource);
    Map<String, Message> messages = new LinkedHashMap<>(log.messages);
    Map<String, EnumDef> enums = new LinkedHashMap<>(log.enums);
    for (Message message : catalog.messages.values()) {
      if (messages.put(message.name, message) != null) {
        throw new IllegalArgumentException("log.proto and catalog.proto both declare " + message.name);
      }
    }
    enums.putAll(catalog.enums);
    for (Map.Entry<String, String> payload : PAYLOADS.entrySet()) {
      String[] at = payload.getKey().split("\\.");
      Message envelope = messages.get(at[0]);
      for (int i = 0; envelope != null && i < envelope.fields.size(); i++) {
        Field field = envelope.fields.get(i);
        if (field.name.equals(at[1])) {
          envelope.fields.set(
              i,
              new Field(
                  field.number, field.name, payload.getValue(), field.repeated, field.packed,
                  field.oneof, field.comment));
        }
      }
    }
    return new Schema(messages, enums);
  }

  /**
   * Parses the subset of proto3 that catalog.proto uses: messages with fields, maps, oneofs,
   * {@code reserved} numbers and the {@code packed} option, and enums. A field's comment is the
   * run of {@code //} lines above it plus any trailing one.
   */
  static Schema parse(String source) {
    Map<String, Message> messages = new LinkedHashMap<>();
    Map<String, EnumDef> enums = new LinkedHashMap<>();
    Message message = null;
    EnumDef enumDef = null;
    String oneof = null;
    StringBuilder pending = new StringBuilder();

    for (String line : stripBlockComments(source).split("\n")) {
      int slash = line.indexOf("//");
      String tail = slash >= 0 ? line.substring(slash + 2).trim() : null;
      String code = slash >= 0 ? line.substring(0, slash) : line;
      if (code.trim().isEmpty()) {
        appendComment(pending, tail);
        continue;
      }

      Matcher m;
      if ((m = MESSAGE_START.matcher(code)).find()) {
        message = new Message(m.group(1));
        messages.put(message.name, message);
        pending.setLength(0);
      } else if ((m = ENUM_START.matcher(code)).find()) {
        enumDef = new EnumDef(m.group(1));
        enums.put(enumDef.name, enumDef);
        pending.setLength(0);
      } else if (message != null && (m = ONEOF_START.matcher(code)).find()) {
        oneof = m.group(1);
        pending.setLength(0);
      } else if (CLOSE.matcher(code).find()) {
        if (oneof != null) {
          oneof = null;
        } else if (enumDef != null) {
          enumDef = null;
        } else {
          message = null;
        }
        pending.setLength(0);
      } else if (enumDef != null && (m = ENUM_VALUE.matcher(code)).find()) {
        enumDef.values.put(m.group(1), Integer.parseInt(m.group(2)));
        pending.setLength(0);
      } else if (message != null && (m = RESERVED.matcher(code)).find()) {
        for (String number : m.group(1).split(",")) {
          message.reserved.add(Integer.parseInt(number.trim()));
        }
        pending.setLength(0);
      } else if (message != null && (m = MAP_FIELD.matcher(code)).find()) {
        message.fields.add(
            new Field(
                Integer.parseInt(m.group(4)),
                m.group(3),
                "map<" + m.group(1) + "," + m.group(2) + ">",
                true,
                false,
                oneof,
                takeComment(pending, tail)));
      } else if (message != null && (m = FIELD.matcher(code)).find()) {
        boolean repeated = m.group(1) != null;
        String options = m.group(6) != null ? m.group(6).replace(" ", "") : "";
        message.fields.add(
            new Field(
                Integer.parseInt(m.group(4)),
                m.group(3),
                m.group(2),
                repeated,
                repeated && !options.contains("packed=false"),
                oneof,
                takeComment(pending, tail)));
      } else {
        pending.setLength(0);
        appendComment(pending, tail);
      }
    }
    return new Schema(messages, enums);
  }

  private static void appendComment(StringBuilder pending, String comment) {
    if (comment != null && !comment.isEmpty()) {
      if (pending.length() > 0) {
        pending.append(' ');
      }
      pending.append(comment);
    }
  }

  private static String takeComment(StringBuilder pending, String tail) {
    appendComment(pending, tail);
    String comment = pending.toString();
    pending.setLength(0);
    return comment;
  }

  private static String stripBlockComments(String source) {
    StringBuilder out = new StringBuilder(source.length());
    int i = 0;
    while (i < source.length()) {
      if (source.startsWith("/*", i)) {
        int end = source.indexOf("*/", i + 2);
        int stop = end < 0 ? source.length() : end + 2;
        for (int j = i; j < stop; j++) {
          if (source.charAt(j) == '\n') {
            out.append('\n');
          }
        }
        i = stop;
      } else {
        out.append(source.charAt(i++));
      }
    }
    return out.toString();
  }

  // ============================================================
  // Conformance
  // ============================================================

  private static final class Violation extends RuntimeException {
    Violation(String message) {
      super(message);
    }
  }

  private static final class Walk {
    private final Schema schema;
    private final byte[] bytes;
    private int pos;

    Walk(Schema schema, byte[] bytes) {
      this.schema = schema;
      this.bytes = bytes;
    }

    void file() {
      if (bytes.length < AtomicFileLog.HEADER_SIZE) {
        throw new Violation("file of " + bytes.length + " bytes has no header");
      }
      for (int i = 0; i < AtomicFileLog.MAGIC.length; i++) {
        if (bytes[i] != AtomicFileLog.MAGIC[i]) {
          throw new Violation("bad magic");
        }
      }
      int version =
          ByteBuffer.wrap(bytes, AtomicFileLog.MAGIC.length, 4).getInt();
      if (version != AtomicFileLog.FORMAT_VERSION) {
        throw new Violation("header version " + version);
      }
      pos = AtomicFileLog.HEADER_SIZE;
      message("LogCheckpoint", "checkpoint", frame("checkpoint"));
      for (int record = 0; pos < bytes.length; record++) {
        String path = "record " + record;
        message("LogRecord", path, frame(path));
      }
    }

    /** Reads a record's length prefix; returns the end of the record. */
    private int frame(String path) {
      long length = varint(bytes.length, path);
      if (length > bytes.length - pos) {
        throw new Violation(path + ": length " + length + " runs past the end of the file");
      }
      return pos + (int) length;
    }

    /** Walks the message of {@code type} from {@link #pos} to {@code end}. */
    private void message(String type, String path, int end) {
      Message message = schema.messages.get(type);
      if (message == null) {
        throw new Violation(path + ": catalog.proto has no message " + type);
      }
      Set<Integer> seen = new HashSet<>();
      Map<String, String> oneofs = new LinkedHashMap<>();
      Map<Integer, Integer> counts = new LinkedHashMap<>();
      while (pos < end) {
        int tagStart = pos;
        long tag = varint(end, path);
        int number = (int) (tag >>> 3);
        int wireType = (int) (tag & 7);
        Field field = message.field(number);
        if (field == null) {
          throw new Violation(
              String.format(
                  "%s: field #%d (wire type %d, at byte %d) is not in %s%s",
                  path,
                  number,
                  wireType,
                  tagStart,
                  type,
                  message.reserved.contains(number) ? " (it is reserved)" : ""));
        }
        int index = counts.merge(number, 1, Integer::sum) - 1;
        String fieldPath = path + "." + field.name + (field.repeated ? "[" + index + "]" : "");
        if (!field.repeated && !seen.add(number)) {
          throw new Violation(fieldPath + ": a singular field appears twice");
        }
        if (field.oneof != null) {
          String other = oneofs.putIfAbsent(field.oneof, field.name);
          if (other != null && !other.equals(field.name)) {
            throw new Violation(
                fieldPath + ": oneof " + field.oneof + " already has " + other);
          }
        }
        value(field, wireType, fieldPath, end);
      }
      if (pos != end) {
        throw new Violation(path + ": the last field runs past the end of the message");
      }
    }

    private void value(Field field, int wireType, String path, int end) {
      String type = field.type;
      if (type.startsWith("map<")) {
        expect(wireType, WIRE_LENGTH_DELIMITED, field, path);
        String[] kv = type.substring(4, type.length() - 1).split(",");
        int entryEnd = lengthDelimited(end, path);
        mapEntry(kv[0], kv[1], path, entryEnd);
        return;
      }
      Integer scalarWire = SCALAR_WIRE_TYPES.get(type);
      if (scalarWire != null) {
        if (field.packed && wireType == WIRE_LENGTH_DELIMITED
            && scalarWire != WIRE_LENGTH_DELIMITED) {
          int packedEnd = lengthDelimited(end, path);
          while (pos < packedEnd) {
            scalar(type, scalarWire, path, packedEnd);
          }
          return;
        }
        expect(wireType, scalarWire, field, path);
        scalar(type, scalarWire, path, end);
        return;
      }
      if (schema.enums.containsKey(type)) {
        expect(wireType, WIRE_VARINT, field, path);
        long value = varint(end, path);
        if (!schema.enums.get(type).values.containsValue((int) value)) {
          throw new Violation(path + ": " + value + " is not a value of " + type);
        }
        return;
      }
      if (schema.messages.containsKey(type)) {
        expect(wireType, WIRE_LENGTH_DELIMITED, field, path);
        int messageEnd = lengthDelimited(end, path);
        message(type, path, messageEnd);
        return;
      }
      throw new Violation(path + ": catalog.proto declares unknown type " + type);
    }

    private void mapEntry(String keyType, String valueType, String path, int end) {
      Message entry = new Message("map entry");
      entry.fields.add(new Field(1, "key", keyType, false, false, null, ""));
      entry.fields.add(new Field(2, "value", valueType, false, false, null, ""));
      Set<Integer> seen = new HashSet<>();
      while (pos < end) {
        long tag = varint(end, path);
        Field field = entry.field((int) (tag >>> 3));
        if (field == null || !seen.add(field.number)) {
          throw new Violation(path + ": a map entry has field #" + (tag >>> 3));
        }
        value(field, (int) (tag & 7), path + "." + field.name, end);
      }
    }

    private void scalar(String type, int wireType, String path, int end) {
      switch (wireType) {
        case WIRE_VARINT:
          int start = pos;
          long value = varint(end, path);
          if (type.equals("bool") && value != 0 && value != 1) {
            throw new Violation(path + ": bool value " + value);
          }
          if (type.endsWith("int32") && !type.startsWith("s")
              && value != (int) value && !fitsShortNegativeInt32(value, pos - start)) {
            throw new Violation(path + ": " + Long.toUnsignedString(value) + " is not an int32");
          }
          return;
        case WIRE_FIXED64:
          skip(8, end, path);
          return;
        case WIRE_FIXED32:
          skip(4, end, path);
          return;
        default:
          int valueEnd = lengthDelimited(end, path);
          if (type.equals("string")) {
            try {
              StandardCharsets.UTF_8
                  .newDecoder()
                  .onMalformedInput(CodingErrorAction.REPORT)
                  .onUnmappableCharacter(CodingErrorAction.REPORT)
                  .decode(ByteBuffer.wrap(bytes, pos, valueEnd - pos));
            } catch (CharacterCodingException e) {
              throw new Violation(path + ": a string is not UTF-8");
            }
          }
          pos = valueEnd;
      }
    }

    /**
     * The codec writes a negative int32 as a 5-byte varint of its 32 bits (catalog.proto's
     * header), which a reader of int32 truncates to the same value.
     */
    private static boolean fitsShortNegativeInt32(long value, int length) {
      return length == 5 && value >= 0x80000000L && value <= 0xFFFFFFFFL;
    }

    private void expect(int wireType, int expected, Field field, String path) {
      if (wireType != expected) {
        throw new Violation(
            String.format(
                "%s: written with wire type %d, but catalog.proto declares %s%s (wire type %d)",
                path, wireType, field.repeated ? "repeated " : "", field.type, expected));
      }
    }

    private int lengthDelimited(int end, String path) {
      long length = varint(end, path);
      if (length > end - pos) {
        throw new Violation(path + ": length " + length + " runs past the enclosing message");
      }
      return pos + (int) length;
    }

    private void skip(int length, int end, String path) {
      if (length > end - pos) {
        throw new Violation(path + ": a fixed-width value runs past the enclosing message");
      }
      pos += length;
    }

    private long varint(int end, String path) {
      long result = 0;
      for (int shift = 0; shift < 64; shift += 7) {
        if (pos >= end) {
          throw new Violation(path + ": a varint runs past the enclosing message");
        }
        byte b = bytes[pos++];
        result |= (long) (b & 0x7F) << shift;
        if ((b & 0x80) == 0) {
          return result;
        }
      }
      throw new Violation(path + ": a varint is longer than 10 bytes");
    }
  }
}
