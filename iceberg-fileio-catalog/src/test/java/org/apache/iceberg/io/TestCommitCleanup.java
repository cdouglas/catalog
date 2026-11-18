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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.exceptions.ServiceUnavailableException;
import org.apache.iceberg.exceptions.ValidationException;
import org.junit.jupiter.api.Test;

/** The deletes of what commits that definitely failed wrote (Bug 119; {@link CommitCleanup}). */
class TestCommitCleanup {
  private final Set<Thread> deleters = ConcurrentHashMap.newKeySet();
  private final InMemoryAtomicFileIO io = new InMemoryAtomicFileIO() {
    @Override
    public void deleteFile(String location) {
      deleters.add(Thread.currentThread());
      super.deleteFile(location);
    }
  };

  /** Files {@code names} under the metadata directory of a table, written. */
  private List<String> written(String... names) {
    List<String> files = new ArrayList<>();
    for (String name : names) {
      String path = "mem:///w/t/metadata/" + name;
      try (PositionOutputStream out = io.newOutputFile(path).createOrOverwrite()) {
        out.write(new byte[] {1});
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
      files.add(path);
    }
    return files;
  }

  private boolean exists(String file) {
    return io.bytes(file) != null;
  }

  @Test
  void aDirectExecutorDeletesOnTheCallersThread() {
    List<String> files = written("a.metadata.json", "snap-1.avro");
    new CommitCleanup(io, Runnable::run).delete(files);

    assertThat(files).noneMatch(this::exists);
    assertThat(deleters).containsExactly(Thread.currentThread());
  }

  @Test
  void theSharedPoolDeletesOffTheCallersThread() throws InterruptedException {
    List<String> files = written("a.metadata.json", "snap-1.avro");
    CommitCleanup cleanup = new CommitCleanup(io, null);
    cleanup.delete(files);

    assertThat(cleanup.await(30_000)).isTrue();
    assertThat(files).noneMatch(this::exists);
    assertThat(deleters).isNotEmpty().doesNotContain(Thread.currentThread())
        .allMatch(thread -> thread.isDaemon()
            && thread.getName().startsWith("fileio-catalog-cleanup-"));
  }

  /** A FileIO with bulk deletes deletes each batch in one call. */
  @Test
  void aBulkFileIODeletesEachBatchInOneCall() {
    List<List<String>> calls = new ArrayList<>();
    FileIO bulk = new Bulk(io, calls);
    List<String> files = written("a.metadata.json", "snap-1.avro", "snap-2.avro");
    new CommitCleanup(bulk, Runnable::run).delete(files);

    assertThat(calls).containsExactly(files);
    assertThat(files).noneMatch(this::exists);
  }

  /** A delete that fails is logged, never thrown, and the batch's other files go. */
  @Test
  void aDeleteThatFailsIsLoggedAndTheOthersGo() {
    List<String> files = written("a.metadata.json", "snap-1.avro");
    io.injectPlainFaults((request, location) ->
        request == InMemoryAtomicFileIO.PlainRequest.DELETE && location.endsWith(".json")
            ? InMemoryAtomicFileIO.Fault.FAIL_BEFORE
            : InMemoryAtomicFileIO.Fault.NONE);
    List<String> missing = List.of("mem:///w/t/metadata/never-written.avro");

    CommitCleanup cleanup = new CommitCleanup(io, Runnable::run);
    cleanup.delete(files);
    cleanup.delete(missing);

    assertThat(exists(files.get(0))).isTrue();
    assertThat(exists(files.get(1))).isFalse();
  }

  /**
   * Closing waits for the deletes handed over, up to its bound, and then drops those not started:
   * their files stay, for a GC by listing; a delete handed over after close deletes nothing.
   */
  @Test
  void closeWaitsForTheDeletesThenDropsTheRest() throws InterruptedException {
    List<String> waitedFor = written("a.metadata.json");
    Executor late = task -> new Thread(() -> {
      try {
        Thread.sleep(200);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      task.run();
    }).start();
    CommitCleanup waiting = new CommitCleanup(io, late, 30_000);
    waiting.delete(waitedFor);
    waiting.close();
    assertThat(waitedFor).noneMatch(this::exists);

    List<Runnable> held = new ArrayList<>();
    List<String> dropped = written("b.metadata.json");
    List<String> afterClose = written("c.metadata.json");
    CommitCleanup holding = new CommitCleanup(io, held::add, 0);
    holding.delete(dropped);
    holding.close();
    held.forEach(Runnable::run);
    holding.delete(afterClose);

    assertThat(held).hasSize(1);
    assertThat(holding.await(0)).as("nothing pending").isTrue();
    assertThat(dropped).allMatch(this::exists);
    assertThat(afterClose).allMatch(this::exists);
  }

  /**
   * A definite failure is one whose record never takes effect: Iceberg's cleanable failures and
   * the catalog's AlreadyExistsException; not an unknown outcome, nor a failure it cannot tell.
   */
  @Test
  void definiteFailures() {
    assertThat(List.<Throwable>of(
            new CommitFailedException("conflict"),
            new NoSuchTableException("gone"),
            new AlreadyExistsException("taken"),
            new RecordTooLargeException(null, "too large"),
            new CommitRefusedException(null, "refused by the store"),
            new ServiceUnavailableException("each attempt conflicted"),
            new ValidationException("staged and refused")))
        .allMatch(CommitCleanup::definitelyFailed);
    assertThat(List.<Throwable>of(
            new CommitStateUnknownException(new RuntimeException("lost reply")),
            new UncheckedIOException(new IOException("read failed")),
            new NotFoundException("missing"),
            new IllegalStateException("unexpected")))
        .noneMatch(CommitCleanup::definitelyFailed);
  }

  /** A FileIO with bulk deletes over {@code io}, recording each call. */
  private static final class Bulk implements FileIO, SupportsBulkOperations {
    private final FileIO io;
    private final List<List<String>> calls;

    Bulk(FileIO io, List<List<String>> calls) {
      this.io = io;
      this.calls = calls;
    }

    @Override
    public InputFile newInputFile(String path) {
      return io.newInputFile(path);
    }

    @Override
    public OutputFile newOutputFile(String path) {
      return io.newOutputFile(path);
    }

    @Override
    public void deleteFile(String path) {
      throw new UnsupportedOperationException("one at a time");
    }

    @Override
    public void deleteFiles(Iterable<String> paths) {
      List<String> batch = new ArrayList<>();
      paths.forEach(batch::add);
      calls.add(batch);
      batch.forEach(io::deleteFile);
    }
  }
}
