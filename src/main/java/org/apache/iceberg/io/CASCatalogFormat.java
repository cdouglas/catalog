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
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

// Old implementation, used in sp24 class project
public class CASCatalogFormat
    implements CatalogFormat<CASCatalogFormat.CASCatalogFile, CASCatalogFormat.Mut> {

  @Override
  public CatalogFile.Mut<CASCatalogFile, Mut> empty(InputFile location) {
    return new Mut(location);
  }

  @Override
  public CatalogFile.Mut<CASCatalogFile, Mut> from(CatalogFile other) {
    return new Mut((CASCatalogFile) other);
  }

  @Override
  public CASCatalogFile read(SupportsAtomicOperations ignored, InputFile catalogLocation) {
    final Map<TableIdentifier, String> fqti = Maps.newHashMap();
    final Map<Namespace, Map<String, String>> namespaces = Maps.newHashMap();
    try (InputStream in = catalogLocation.newStream();
        DataInputStream din = new DataInputStream(in)) {
      int nNamespaces = din.readInt();
      for (int i = 0; i < nNamespaces; ++i) {
        Namespace namespace = readNamespace(din);
        Map<String, String> props = readProperties(din);
        namespaces.put(namespace, props);
      }
      int nTables = din.readInt();
      for (int i = 0; i < nTables; i++) {
        Namespace namespace = readNamespace(din);
        TableIdentifier tid = TableIdentifier.of(namespace, din.readUTF());
        fqti.put(tid, din.readUTF()); // location
      }
      long msb = din.readLong();
      long lsb = din.readLong();
      return new CASCatalogFile(new UUID(msb, lsb), namespaces, fqti, catalogLocation);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  // TODO revisit visibility, this is haphazard nonsense.
  public static class CASCatalogFile extends CatalogFile {
    private final Map<TableIdentifier, String> tblLocations;
    private final Map<Namespace, Map<String, String>> namespaces;

    // TODO find a way to document/constrain empty NS in empty catalogs
    CASCatalogFile(InputFile location) {
      super(location);
      this.tblLocations = Maps.newHashMap();
      this.namespaces = Maps.newHashMap();
      namespaces.put(Namespace.empty(), Maps.newHashMap());
    }

    CASCatalogFile(
        UUID uuid,
        Map<Namespace, Map<String, String>> namespaces,
        Map<TableIdentifier, String> tblLocations,
        InputFile location) {
      super(uuid, location);
      this.tblLocations = tblLocations;
      this.namespaces = namespaces;
    }

    @Override
    public boolean createsHierarchicalNamespaces() {
      return false;
    }

    @Override
    public String location(TableIdentifier table) {
      return tblLocations.get(table);
    }

    @Override
    public Set<Namespace> namespaces() {
      return Collections.unmodifiableSet(namespaces.keySet());
    }

    @Override
    public boolean containsNamespace(Namespace namespace) {
      return namespaces.containsKey(namespace);
    }

    @Override
    public Map<String, String> namespaceProperties(Namespace namespace) {
      return Collections.unmodifiableMap(namespaces.get(namespace));
    }

    @Override
    public List<TableIdentifier> tables() {
      return Lists.newArrayList(tblLocations.keySet().iterator());
    }

    @Override
    Map<Namespace, Map<String, String>> namespaceProperties() {
      return Collections.unmodifiableMap(namespaces);
    }

    @Override
    Map<TableIdentifier, String> locations() {
      return Collections.unmodifiableMap(tblLocations);
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (other == null || getClass() != other.getClass()) {
        return false;
      }
      CASCatalogFile that = (CASCatalogFile) other;
      return uuid().equals(that.uuid())
          && tblLocations.equals(that.tblLocations)
          && namespaces.equals(that.namespaces);
    }

    @Override
    public int hashCode() {
      // TODO replace with CRC during deserialization?
      return Objects.hash(uuid(), tblLocations.keySet(), namespaces.keySet());
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("{");
      sb.append("\"uuid\" : \"").append(uuid()).append("\",");
      sb.append("\"tables\" : [");
      sb.append(
              tblLocations.keySet().stream()
                  .map(id -> "\"" + id + "\"")
                  .collect(Collectors.joining(",")))
          .append("],");
      sb.append("\"namespaces\" : [");
      sb.append(
          namespaces.keySet().stream()
              .map(id -> "\"" + id + "\"")
              .collect(Collectors.joining(",")));
      sb.append("]").append("}");
      return sb.toString();
    }
  }

  public static class Mut extends CatalogFile.Mut<CASCatalogFile, Mut> {
    Mut(InputFile location) {
      this(new CASCatalogFile(location));
    }

    Mut(CASCatalogFile original) {
      super(original);
    }

    @Override
    public CASCatalogFile commit(SupportsAtomicOperations fileIO) {
      try {
        CatalogFile catalog = merge();
        final AtomicOutputFile outputFile = fileIO.newOutputFile(original.location());
        try {
          byte[] ffs = asBytes(catalog);
          try (ByteArrayInputStream serBytes = new ByteArrayInputStream(asBytes(catalog))) {
            serBytes.mark(ffs.length); // readAheadLimit ignored, but whatever
            CAS token = outputFile.prepare(() -> serBytes, AtomicOutputFile.Strategy.CAS);
            serBytes.reset();
            InputFile newCatalog = outputFile.writeAtomic(token, () -> serBytes);
            return new CASCatalogFile(
                catalog.uuid(), catalog.namespaceProperties(), catalog.locations(), newCatalog);
          }
        } catch (IOException e) {
          throw new CommitFailedException(e, "Failed to commit catalog file");
        }
      } catch (SupportsAtomicOperations.CASException e) {
        throw new CommitFailedException(e, "Cannot commit");
      }
    }

    protected CatalogFile merge() {
      final Map<Namespace, Map<String, String>> nsProp =
          Maps.newHashMap(original.namespaceProperties());
      merge(
          nsProp,
          namespaceProperties,
          (orig, next) -> {
            Map<String, String> nsProps = null == orig ? Maps.newHashMap() : Maps.newHashMap(orig);
            merge(nsProps, next, (x, y) -> y);
            return nsProps;
          });
      for (Map.Entry<Namespace, Boolean> entry : namespaces.entrySet()) {
        if (!entry.getValue()) {
          nsProp.remove(entry.getKey());
        }
      }

      final Map<TableIdentifier, String> newTbl = Maps.newHashMap(original.locations());
      merge(newTbl, tables, (x, location) -> location);
      merge(newTbl, tableUpdates, (x, location) -> location);
      return new CASCatalogFile(original.uuid(), nsProp, newTbl, original.location());
    }

    /**
     * Given two maps o, u, overwrite values where u[k] is defined and remove them where u[k] ==
     * null
     */
    private static <K, V, U> void merge(
        Map<K, V> original, Map<K, U> update, BiFunction<V, U, V> valueMapper) {
      for (Map.Entry<K, U> entry : update.entrySet()) {
        final K key = entry.getKey();
        final U value = entry.getValue();
        if (null == value) {
          original.remove(key);
        } else {
          original.put(key, valueMapper.apply(original.get(key), value));
        }
      }
    }
  }

  static byte[] asBytes(CatalogFile file) {
    // TODO unnecessary buffer copy; DataInput/DataOutputStream avail?
    try (ByteArrayOutputStream bytes = new ByteArrayOutputStream(2048)) {
      write(file, bytes);
      return bytes.toByteArray();
    } catch (IOException e) {
      throw new CommitFailedException(e, "Failed to commit catalog file");
    }
  }

  static void write(CatalogFile file, OutputStream out) throws IOException {
    try (DataOutputStream dos = new DataOutputStream(out)) {
      // namespaces
      Map<Namespace, Map<String, String>> namespaces = file.namespaceProperties();
      dos.writeInt(namespaces.size());
      for (Map.Entry<Namespace, Map<String, String>> e : namespaces.entrySet()) {
        writeNamespace(dos, e.getKey());
        writeProperties(dos, e.getValue());
      }
      Map<TableIdentifier, String> locations = file.locations();
      dos.writeInt(locations.size());
      for (Map.Entry<TableIdentifier, String> e : locations.entrySet()) {
        TableIdentifier tid = e.getKey();
        writeNamespace(dos, tid.namespace());
        dos.writeUTF(tid.name());
        dos.writeUTF(e.getValue());
      }
      // table uuid
      UUID uuid = file.uuid();
      dos.writeLong(uuid.getMostSignificantBits());
      dos.writeLong(uuid.getLeastSignificantBits());
    }
  }

  private static Map<String, String> readProperties(DataInputStream in) throws IOException {
    int nprops = in.readInt();
    Map<String, String> props = nprops > 0 ? Maps.newHashMap() : Collections.emptyMap();
    for (int j = 0; j < nprops; j++) {
      props.put(in.readUTF(), in.readUTF());
    }
    return props;
  }

  private static Namespace readNamespace(DataInputStream in) throws IOException {
    int nlen = in.readInt();
    String[] levels = new String[nlen];
    for (int j = 0; j < nlen; j++) {
      levels[j] = in.readUTF();
    }
    return Namespace.of(levels);
  }

  static void writeNamespace(DataOutputStream out, Namespace namespace) throws IOException {
    out.writeInt(namespace.length());
    for (String n : namespace.levels()) {
      out.writeUTF(n);
    }
  }

  static void writeProperties(DataOutputStream out, Map<String, String> props) throws IOException {
    Map<String, String> writeProps =
        props.entrySet().stream()
            .filter(e -> e.getValue() != null)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    out.writeInt(writeProps.size());
    for (Map.Entry<String, String> p : writeProps.entrySet()) {
      out.writeUTF(p.getKey());
      out.writeUTF(p.getValue());
    }
  }
}
