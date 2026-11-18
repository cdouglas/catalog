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
package org.apache.iceberg.io.bench;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.zip.Deflater;
import java.util.zip.GZIPOutputStream;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.EnvironmentContext;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.view.BaseView;
import org.apache.iceberg.view.View;
import org.apache.iceberg.view.ViewBuilder;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.iceberg.view.ViewMetadataParser;

/**
 * M1 of docs/VIEWS.md: what a view costs the catalog in bytes, measured with Iceberg's own
 * {@link ViewMetadataParser} on views made through Iceberg's view API ({@link InMemoryCatalog}'s
 * {@code buildView}, with the summary Iceberg puts on every version and the properties Spark's
 * {@code CREATE VIEW} adds). Each version's SQL is distinct.
 *
 * <p>For each view it prints the metadata's JSON and that JSON gzipped: at the default level, as a
 * pointer view's metadata file holds it (views default to gzip, {@code
 * ViewProperties.METADATA_COMPRESSION_DEFAULT}) and as the inline form does, and at the fastest,
 * which tables' inline codec uses (a checkpoint re-encodes tables, never a view); then the bytes of
 * the actions the design proposes, on the wire as {@code ProtoCodec} writes them (a negative int32
 * in five bytes, codec fields written when 0): a create, a replace (one more version) and a
 * property change, each inline, by a full pointer and by a compact one (errata D28: a 16-byte nonce
 * and the version); and a checkpoint's {@code View} row in each form. Last, a checkpoint's views at
 * Polaris' default tree (24 views) and at 10,000.
 *
 * <p>The record bytes are the action inside its {@code Intention} ({@code Action} in {@code
 * actions}); the log's envelope around it (record id, frame) is the same for any record and is left
 * out.
 *
 * <p>Run via:
 *
 * <pre>{@code
 * mvn -pl iceberg-fileio-catalog test-compile exec:java \
 *   -Dexec.mainClass=org.apache.iceberg.io.bench.ViewMetadataSizeProbe \
 *   -Dexec.classpathScope=test
 * }</pre>
 */
public final class ViewMetadataSizeProbe {

  private static final String WAREHOUSE = "s3://analytics-prod-lake/warehouse";
  private static final Namespace NAMESPACE = Namespace.of("sales_db");

  private static final String[] WORDS = {
    "order", "customer", "region", "amount", "price", "quantity", "status", "created", "updated",
    "shipped", "product", "category", "discount", "tax", "total", "store", "channel", "country",
    "currency", "payment", "invoice", "account", "segment", "revenue", "margin", "cost", "event",
    "session", "device", "campaign"
  };
  private static final String[] SUFFIXES = {"id", "ts", "name", "code", "amt", "flag", "dt", "key"};
  private static final Type[] TYPES = {
    Types.LongType.get(),
    Types.StringType.get(),
    Types.DecimalType.of(18, 2),
    Types.TimestampType.withZone(),
    Types.DateType.get(),
    Types.BooleanType.get(),
    Types.DoubleType.get(),
    Types.IntegerType.get()
  };

  private ViewMetadataSizeProbe() {}

  /**
   * A view's shape: columns, SQL bytes per version, versions, schemas over them, and whether each
   * version's SQL is new ({@code edited} false) or the last version's with one predicate changed.
   */
  record Shape(int columns, int sqlBytes, int versions, int schemas, boolean edited) {
    String label() {
      return String.format(
          Locale.ROOT,
          "%d cols, %s SQL, %d version%s%s%s",
          columns,
          sqlBytes >= 1000 ? (sqlBytes / 1000) + " KB" : sqlBytes + " B",
          versions,
          versions == 1 ? "" : "s",
          versions == 1 ? "" : edited ? " edited" : " distinct",
          schemas > 1 ? ", " + schemas + " schemas" : "");
    }
  }

  /** A view's metadata at its first version, its last, and its last with a property set. */
  record Measured(
      Shape shape, String name, ViewMetadata last, ViewMetadata first, ViewMetadata withProperty) {}

  public static void main(String[] args) {
    EnvironmentContext.put(EnvironmentContext.ENGINE_NAME, "spark");
    EnvironmentContext.put(EnvironmentContext.ENGINE_VERSION, "4.0.0");

    List<Shape> shapes = new ArrayList<>();
    for (int columns : new int[] {5, 10, 50}) {
      for (int sql : new int[] {100, 1000, 10_000}) {
        shapes.add(new Shape(columns, sql, 1, 1, false));
        shapes.add(new Shape(columns, sql, 10, 1, false));
        shapes.add(new Shape(columns, sql, 10, 1, true));
        shapes.add(new Shape(columns, sql, 10, 3, true));
      }
    }

    InMemoryCatalog catalog = new InMemoryCatalog();
    catalog.initialize("probe", Map.of(CatalogProperties.WAREHOUSE_LOCATION, WAREHOUSE));
    catalog.createNamespace(NAMESPACE);

    List<Measured> measured = new ArrayList<>();
    int viewNo = 0;
    for (Shape shape : shapes) {
      measured.add(measure(catalog, shape, viewNo++));
    }

    System.out.println("## The metadata\n");
    System.out.println(
        "| View | JSON | gzip | gzip, fastest | per version |");
    System.out.println("|---|---:|---:|---:|---:|");
    for (Measured m : measured) {
      byte[] json = json(m.last());
      int inline = inlineBytes(m.last());
      String perVersion =
          m.shape().versions() > 1
              ? String.valueOf((inline - inlineBytes(m.first())) / (m.shape().versions() - 1))
              : "";
      System.out.printf(
          Locale.ROOT,
          "| %s | %,d | %,d | %,d | %s |%n",
          m.shape().label(),
          json.length,
          inline,
          gzip(json, Deflater.BEST_SPEED).length,
          perVersion);
    }

    System.out.println("\n## Records (the action in its intention), bytes\n");
    System.out.println(
        "| View | create inline | create pointer | replace inline | replace pointer"
            + " | properties inline | compact pointer, any |");
    System.out.println("|---|---:|---:|---:|---:|---:|---:|");
    for (Measured m : measured) {
      String name = viewName(m);
      String pointer = pointer(name, m.shape().versions());
      System.out.printf(
          Locale.ROOT,
          "| %s | %,d | %,d | %,d | %,d | %,d | %,d |%n",
          m.shape().label(),
          createView(name, inlineBytes(m.first())),
          createView(name, pointer.getBytes(StandardCharsets.UTF_8).length),
          updateView(inlineBytes(m.last())),
          updateView(pointer.getBytes(StandardCharsets.UTF_8).length),
          updateView(inlineBytes(m.withProperty())),
          updateView(COMPACT_POINTER_BYTES));
    }

    System.out.println("\n## A checkpoint's View row, bytes\n");
    System.out.println("| View | inline | full pointer | compact pointer |");
    System.out.println("|---|---:|---:|---:|");
    for (Measured m : measured) {
      String name = viewName(m);
      System.out.printf(
          Locale.ROOT,
          "| %s | %,d | %,d | %,d |%n",
          m.shape().label(),
          viewRow(name, inlineBytes(m.last()), true),
          viewRow(name, pointer(name, m.shape().versions()).length(), false),
          viewRow(name, COMPACT_POINTER_BYTES, false));
    }

    System.out.println("\n## A checkpoint's views, bytes\n");
    System.out.println("| Views | of shape | inline | full pointers | compact pointers |");
    System.out.println("|---:|---|---:|---:|---:|");
    for (int count : new int[] {24, 10_000}) {
      for (Shape typical :
          List.of(
              new Shape(10, 1000, 1, 1, false),
              new Shape(10, 1000, 10, 1, true),
              new Shape(10, 1000, 10, 1, false),
              new Shape(50, 10_000, 10, 3, true))) {
        Measured m =
            measured.stream().filter(x -> x.shape().equals(typical)).findFirst().orElseThrow();
        String name = viewName(m);
        long inline = (long) count * viewRow(name, inlineBytes(m.last()), true);
        long full = (long) count * viewRow(name, pointer(name, typical.versions()).length(), false);
        long compact = (long) count * viewRow(name, COMPACT_POINTER_BYTES, false);
        System.out.printf(
            Locale.ROOT,
            "| %,d | %s | %s | %s | %s |%n",
            count,
            typical.label(),
            human(inline),
            human(full),
            human(compact));
      }
    }

    String example = pointer(viewName(measured.get(0)), 3);
    System.out.printf(
        Locale.ROOT, "%nA full pointer: %s (%d B)%n", example, example.length());
  }

  /** A compact pointer: a 16-byte nonce and the version's varint, under the view's location. */
  private static final int COMPACT_POINTER_BYTES = 16 + 1;

  private static Measured measure(InMemoryCatalog catalog, Shape shape, int viewNo) {
    TableIdentifier id = TableIdentifier.of(NAMESPACE, viewName(viewNo));
    Random random = new Random(31L * viewNo + 7);
    List<Types.NestedField> columns = columns(random, shape.columns());
    int schemaChanges = shape.schemas() - 1;
    View view = null;
    ViewMetadata first = null;
    Query query = null;
    for (int v = 1; v <= shape.versions(); v++) {
      // a new schema at evenly spaced versions: a column added
      int schemaNo = schemaChanges == 0 ? 0 : (v - 1) * shape.schemas() / shape.versions();
      List<Types.NestedField> fields = new ArrayList<>(columns);
      for (int s = 0; s < schemaNo; s++) {
        fields.add(
            Types.NestedField.optional(
                columns.size() + s + 1, word(random) + "_added_" + s, Types.StringType.get()));
      }
      Schema schema = new Schema(fields);
      query =
          query == null || !shape.edited()
              ? Query.random(random, fields, shape.sqlBytes())
              : query.edited(random, fields);
      ViewBuilder builder =
          catalog
              .buildView(id)
              .withSchema(schema)
              .withDefaultCatalog("prod")
              .withDefaultNamespace(NAMESPACE)
              .withQuery("spark", query.toString())
              .withProperties(sparkProperties(fields));
      view = v == 1 ? builder.create() : builder.replace();
      if (v == 1) {
        first = current(view);
      }
    }
    ViewMetadata last = current(view);
    view.updateProperties().set("comment", "Daily revenue by region and channel").commit();
    ViewMetadata withProperty = current(view);
    return new Measured(shape, id.name(), last, first, withProperty);
  }

  private static ViewMetadata current(View view) {
    return ((BaseView) view).operations().current();
  }

  private static List<Types.NestedField> columns(Random random, int count) {
    List<Types.NestedField> fields = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      String name =
          word(random) + "_" + SUFFIXES[random.nextInt(SUFFIXES.length)] + (i >= 20 ? "_" + i : "");
      fields.add(Types.NestedField.optional(i + 1, name + "_" + i, TYPES[random.nextInt(TYPES.length)]));
    }
    return fields;
  }

  private static String word(Random random) {
    return WORDS[random.nextInt(WORDS.length)];
  }

  /** A query: a select list and joins, then predicates, about {@code bytes} in all. */
  record Query(String head, List<String> predicates) {
    static Query random(Random random, List<Types.NestedField> fields, int bytes) {
      StringBuilder head = new StringBuilder("SELECT ");
      for (int i = 0; i < fields.size(); i++) {
        if (i > 0) {
          head.append(", ");
        }
        head.append("t").append(random.nextInt(3)).append('.').append(fields.get(i).name());
        if (head.length() > bytes / 2) {
          break;
        }
      }
      head.append(" FROM prod.sales_db.")
          .append(word(random))
          .append("_facts t0 JOIN prod.sales_db.")
          .append(word(random))
          .append("_dim t1 ON t0.")
          .append(word(random))
          .append("_id = t1.")
          .append(word(random))
          .append("_id WHERE ");
      List<String> predicates = new ArrayList<>();
      int length = head.length();
      while (predicates.isEmpty() || length < bytes) {
        String predicate = (predicates.isEmpty() ? "" : " AND ") + predicate(random, fields);
        predicates.add(predicate);
        length += predicate.length();
      }
      return new Query(head.toString(), predicates);
    }

    /** This query with one predicate changed. */
    Query edited(Random random, List<Types.NestedField> fields) {
      List<String> changed = new ArrayList<>(predicates);
      int at = random.nextInt(changed.size());
      changed.set(at, (at == 0 ? "" : " AND ") + predicate(random, fields));
      return new Query(head, changed);
    }

    private static String predicate(Random random, List<Types.NestedField> fields) {
      String name = fields.get(random.nextInt(fields.size())).name();
      return switch (random.nextInt(4)) {
        case 0 -> "t0." + name + " > " + random.nextInt(100_000);
        case 1 -> "t1." + name + " IN ('" + word(random) + "', '" + word(random) + "')";
        case 2 -> "t0." + name + " IS NOT NULL";
        default -> String.format(
            Locale.ROOT,
            "date_trunc('day', t0.%s) >= DATE '2026-%02d-%02d'",
            name,
            1 + random.nextInt(12),
            1 + random.nextInt(28));
      };
    }

    @Override
    public String toString() {
      return head + String.join("", predicates);
    }
  }

  /** What Spark's {@code CREATE VIEW} adds (Iceberg's ViewUtil.createProperties). */
  private static Map<String, String> sparkProperties(List<Types.NestedField> fields) {
    List<String> names = fields.stream().map(Types.NestedField::name).toList();
    Map<String, String> props = new LinkedHashMap<>();
    props.put("spark.query-column-names", String.join(",", names));
    props.put(
        "spark.query-column-names-json",
        "[" + String.join(",", names.stream().map(n -> "\"" + n + "\"").toList()) + "]");
    props.put("create_engine_version", "Spark 4.0.0");
    props.put("engine_version", "Spark 4.0.0");
    return props;
  }

  private static String viewName(int viewNo) {
    return "daily_revenue_v" + viewNo;
  }

  private static String viewName(Measured m) {
    return m.name();
  }

  /** A pointer view's metadata file, as BaseViewOperations names it. */
  private static String pointer(String viewName, int version) {
    return String.format(
        Locale.ROOT,
        "%s/%s/%s/metadata/%05d-%s.gz.metadata.json",
        WAREHOUSE,
        NAMESPACE,
        viewName,
        version,
        UUID.nameUUIDFromBytes(viewName.getBytes(StandardCharsets.UTF_8)));
  }

  private static byte[] json(ViewMetadata metadata) {
    return ViewMetadataParser.toJson(metadata).getBytes(StandardCharsets.UTF_8);
  }

  /** Inline, as a pointer view's file: gzip at the default level (tables' inline codec: fastest). */
  private static int inlineBytes(ViewMetadata metadata) {
    return gzip(json(metadata), Deflater.DEFAULT_COMPRESSION).length;
  }

  private static byte[] gzip(byte[] in, int level) {
    try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      try (GZIPOutputStream gz =
          new GZIPOutputStream(out) {
            {
              def.setLevel(level);
            }
          }) {
        gz.write(in);
      }
      return out.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  // ---- the proposed actions on the wire (VIEWS.md "Format"), as ProtoCodec writes them ----

  private static int varint(long value) {
    int size = 1;
    while ((value & ~0x7FL) != 0) {
      value >>>= 7;
      size++;
    }
    return size;
  }

  /** An int32 field: a negative one in five bytes, as ProtoCodec.writeRawVarint(int). */
  private static int int32Field(int value) {
    return 1 + (value < 0 ? 5 : varint(value));
  }

  private static int lenField(int fieldNumber, int length) {
    return varint((long) fieldNumber << 3) + varint(length) + length;
  }

  /** An action in an Intention: actions (3) of Action, its oneof case {@code field}. */
  private static int inIntention(int actionField, int actionBytes) {
    return lenField(3, lenField(actionField, actionBytes));
  }

  /**
   * CreateView (Action 14): id -1, version 1, namespace id, removals, name, metadata, and a codec
   * with inline metadata.
   */
  private static int createView(String name, int metadataBytes) {
    int body =
        int32Field(-1)
            + int32Field(1)
            + int32Field(3)
            + int32Field(0)
            + lenField(6, name.length())
            + lenField(7, metadataBytes)
            + int32Field(0);
    return inIntention(14, body);
  }

  /** UpdateView (Action 15): id, version, metadata, codec. */
  private static int updateView(int metadataBytes) {
    int body = int32Field(12) + int32Field(4) + lenField(3, metadataBytes) + int32Field(0);
    return inIntention(15, body);
  }

  /** A checkpoint's View row (Checkpoint.views, 14). */
  private static int viewRow(String name, int metadataBytes, boolean inline) {
    int body =
        int32Field(12)
            + int32Field(4)
            + int32Field(3)
            + lenField(4, name.length())
            + lenField(5, metadataBytes)
            + (inline ? int32Field(0) : 0);
    return lenField(14, body);
  }

  private static String human(long bytes) {
    if (bytes >= 1 << 20) {
      return String.format(Locale.ROOT, "%.1f MB", bytes / (double) (1 << 20));
    } else if (bytes >= 1 << 10) {
      return String.format(Locale.ROOT, "%.1f KB", bytes / (double) (1 << 10));
    }
    return bytes + " B";
  }
}
