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
package org.intentlog.azure;

import java.util.List;
import java.util.Map;
import org.intentlog.azure.InMemoryAppendBlobStore.Appended;
import org.intentlog.azure.InMemoryAppendBlobStore.Interceptor;

/**
 * A store whose requests a fuzzer can fault, and which says what landed in it: every append, every
 * version of every blob put. {@link InMemoryAppendBlobStore} is one ({@link #of}); {@link
 * RecordingStore} makes one of any store, a real account's included.
 */
interface ObservedStore extends AppendBlobStore {

  /** Routes every request through {@code hooks} (null: none). */
  void intercept(Interceptor hooks);

  /** Every append that landed, in order. */
  List<Appended> appends();

  /** Every version of every blob put (an Append Blob's creation as an empty one), by name. */
  Map<String, List<byte[]>> versions();

  /** The blob's bytes now, read without hooks; null if it does not exist. */
  byte[] peek(String name);

  /** The names of the blobs that exist now. */
  List<String> names();

  /** Lands the appends delayed so far, each if its conditions hold; returns how many landed. */
  int deliverDelayed();

  static ObservedStore of(InMemoryAppendBlobStore s) {
    return new ObservedStore() {
      @Override
      public void intercept(Interceptor hooks) {
        s.intercept(hooks);
      }

      @Override
      public List<Appended> appends() {
        return s.appends();
      }

      @Override
      public Map<String, List<byte[]>> versions() {
        return s.versions();
      }

      @Override
      public byte[] peek(String name) {
        return s.peek(name);
      }

      @Override
      public List<String> names() {
        return s.names();
      }

      @Override
      public int deliverDelayed() {
        return s.deliverDelayed();
      }

      @Override
      public boolean createAppendBlob(String name, Map<String, String> metadata) {
        return s.createAppendBlob(name, metadata);
      }

      @Override
      public long appendBlock(String name, byte[] block, long maxSize, long appendPosition) {
        return s.appendBlock(name, block, maxSize, appendPosition);
      }

      @Override
      public Blob read(String name, long from) {
        return s.read(name, from);
      }

      @Override
      public Props properties(String name) {
        return s.properties(name);
      }

      @Override
      public boolean putIfAbsent(String name, byte[] bytes) {
        return s.putIfAbsent(name, bytes);
      }

      @Override
      public void put(String name, byte[] bytes) {
        s.put(name, bytes);
      }

      @Override
      public String putIfMatch(String name, byte[] bytes, String etag) {
        return s.putIfMatch(name, bytes, etag);
      }

      @Override
      public void delete(String name) {
        s.delete(name);
      }

      @Override
      public List<Entry> list(String prefix) {
        return s.list(prefix);
      }

      @Override
      public String location(String name) {
        return s.location(name);
      }

      @Override
      public int maxBlocks() {
        return s.maxBlocks();
      }

      @Override
      public long maxBlockBytes() {
        return s.maxBlockBytes();
      }
    };
  }
}
