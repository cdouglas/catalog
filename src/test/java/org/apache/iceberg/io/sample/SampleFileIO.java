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
package org.apache.iceberg.io.sample;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import org.apache.iceberg.inmemory.InMemoryInputFile;
import org.apache.iceberg.io.AtomicOutputFile;
import org.apache.iceberg.io.CAS;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.io.SupportsAtomicOperations;

/**
 * In-memory {@link SupportsAtomicOperations} that records every written file's
 * exact bytes, used as the measurement substrate for the footprint samples.
 * Single-threaded (no contention), so CAS/APPEND need not validate preconditions —
 * a faithful port of the proven {@code TestInlineManifestEndToEnd.MemoryFileIO}.
 */
public final class SampleFileIO implements SupportsAtomicOperations {

  public final ConcurrentHashMap<String, byte[]> files = new ConcurrentHashMap<>();

  @Override
  public InputFile newInputFile(String location) {
    return new InputFile() {
      @Override
      public long getLength() {
        byte[] b = files.get(location);
        return b != null ? b.length : 0;
      }

      @Override
      public SeekableInputStream newStream() {
        byte[] b = files.get(location);
        if (b == null) {
          throw new RuntimeException("File not found: " + location);
        }
        return new InMemoryInputFile(location, b).newStream();
      }

      @Override
      public String location() {
        return location;
      }

      @Override
      public boolean exists() {
        return files.containsKey(location);
      }
    };
  }

  @Override
  public OutputFile newOutputFile(String location) {
    SampleFileIO self = this;
    return new OutputFile() {
      @Override
      public PositionOutputStream create() {
        return new MemoryOutputStream(location, files);
      }

      @Override
      public PositionOutputStream createOrOverwrite() {
        return create();
      }

      @Override
      public String location() {
        return location;
      }

      @Override
      public InputFile toInputFile() {
        return self.newInputFile(location);
      }
    };
  }

  @Override
  public void deleteFile(String location) {
    files.remove(location);
  }

  @Override
  public AtomicOutputFile newOutputFile(InputFile replace) {
    String location = replace.location();
    SampleFileIO self = this;
    return new AtomicOutputFile() {
      private AtomicOutputFile.Strategy lastStrategy;

      @Override
      public CAS prepare(Supplier<InputStream> content, AtomicOutputFile.Strategy strategy) {
        this.lastStrategy = strategy;
        return new CAS() {
          @Override
          public long contentLength() {
            return 0;
          }

          @Override
          public byte[] contentChecksumBytes() {
            return new byte[0];
          }

          @Override
          public String contentHeaderString() {
            return "";
          }
        };
      }

      @Override
      public InputFile writeAtomic(CAS cas, Supplier<InputStream> content) throws IOException {
        byte[] data = readAll(content.get());
        if (lastStrategy == Strategy.APPEND) {
          files.merge(
              location,
              data,
              (old, added) -> {
                byte[] merged = new byte[old.length + added.length];
                System.arraycopy(old, 0, merged, 0, old.length);
                System.arraycopy(added, 0, merged, old.length, added.length);
                return merged;
              });
        } else {
          files.put(location, data);
        }
        return self.newInputFile(location);
      }

      @Override
      public PositionOutputStream create() {
        return new MemoryOutputStream(location, files);
      }

      @Override
      public PositionOutputStream createOrOverwrite() {
        return create();
      }

      @Override
      public String location() {
        return location;
      }

      @Override
      public InputFile toInputFile() {
        return self.newInputFile(location);
      }
    };
  }

  private static byte[] readAll(InputStream is) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    byte[] buf = new byte[4096];
    int n;
    while ((n = is.read(buf)) > 0) {
      out.write(buf, 0, n);
    }
    is.close();
    return out.toByteArray();
  }

  /** PositionOutputStream backed by a byte array, stored to the file map on close. */
  static final class MemoryOutputStream extends PositionOutputStream {
    private final String location;
    private final ConcurrentHashMap<String, byte[]> files;
    private final ByteArrayOutputStream buf = new ByteArrayOutputStream();
    private long pos = 0;

    MemoryOutputStream(String location, ConcurrentHashMap<String, byte[]> files) {
      this.location = location;
      this.files = files;
    }

    @Override
    public long getPos() {
      return pos;
    }

    @Override
    public void write(int b) {
      buf.write(b);
      pos++;
    }

    @Override
    public void write(byte[] b, int off, int len) {
      buf.write(b, off, len);
      pos += len;
    }

    @Override
    public void close() throws IOException {
      super.close();
      files.put(location, buf.toByteArray());
    }
  }
}
