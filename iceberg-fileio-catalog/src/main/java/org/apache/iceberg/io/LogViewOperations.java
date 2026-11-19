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

import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.view.BaseViewOperations;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.iceberg.view.ViewMetadataParser;
import org.intentlog.Replica;

/**
 * Operations on a view of a {@link LogCatalog} (docs/VIEWS.md). Each holds the read of the
 * catalog's log its view was loaded from, which a commit to the view is staged on.
 *
 * <p>A commit sends the view's whole metadata (V2): inline, Iceberg's view JSON gzipped ({@link
 * InlineViewMetadata}), if the catalog inlines ({@code fileio.catalog.inline}) and it fits in
 * {@code fileio.catalog.inline.max.update.size}; else a metadata file it writes first, whose
 * location the record names. A registered view always names its file (V3). A create is staged on a
 * fresh read again after a conflict, as a table's is; an update applies only on the version it read
 * (V6), and its conflict fails the commit, which Iceberg's view updates retry on a fresh read. What
 * a commit that definitely failed wrote goes to the catalog's cleanup ({@link CommitCleanup}).
 */
final class LogViewOperations extends BaseViewOperations {
  private final TableIdentifier view;
  private final String fullName;
  private final CatalogLog log;
  private final FileIO io;
  private final boolean inline;
  private final int maxUpdateSize;
  private final CommitCleanup cleanup;
  private final CatalogMutation.CreateCondition createCondition;
  private volatile Replica<CatalogState> lastRead = null;
  // The replica the last commit through these operations landed in, and the metadata it sent,
  // until commit() adopts them
  private Replica<CatalogState> landed = null;
  private ViewMetadata sent = null;
  private String sentLocation = null;

  LogViewOperations(
      TableIdentifier view,
      String fullName,
      CatalogLog log,
      FileIO io,
      boolean inline,
      int maxUpdateSize,
      CommitCleanup cleanup,
      CatalogMutation.CreateCondition createCondition) {
    this.view = view;
    this.fullName = fullName;
    this.log = log;
    this.io = io;
    this.inline = inline;
    this.maxUpdateSize = maxUpdateSize;
    this.cleanup = cleanup;
    this.createCondition = createCondition;
  }

  @Override
  protected String viewName() {
    return fullName;
  }

  @Override
  protected FileIO io() {
    return io;
  }

  /**
   * Loads the view as a fresh read of the catalog holds it. A pointer view's metadata file deleted
   * by a drop that landed after the read is read again from a fresh read ({@link
   * BaseViewOperations}'s refresh, on the {@link org.apache.iceberg.exceptions.NotFoundException}).
   */
  @Override
  protected void doRefresh() {
    Replica<CatalogState> read = log.read();
    loadFrom(read.state(), null);
    lastRead = read;
  }

  /**
   * Loads the view from {@code state}: inline, by decoding it; a pointer, by reading its file,
   * unless {@code written} is the metadata these operations wrote to the file {@code state} names.
   * An inline view's location names its id and version ({@link CatalogState#inlineViewLocation}),
   * so a load of an unchanged view reads nothing and one of a view that held the name before it
   * never matches.
   */
  private void loadFrom(CatalogState state, ViewMetadata written) {
    CatalogState.ViewEntry entry = state.viewEntry(view);
    if (entry == null) {
      if (currentMetadataLocation() != null) {
        throw new NoSuchViewException("View does not exist: %s", view);
      }
      disableRefresh();
      return;
    }
    if (entry.isInline()) {
      String location = CatalogState.inlineViewLocation(view, state.viewId(view), entry.version);
      byte[] bytes = entry.inlineMetadata();
      byte codec = entry.codec;
      refreshFromMetadataLocation(
          location, null, 0, at -> InlineViewMetadata.decode(bytes, codec, at));
    } else if (written != null && entry.metadataLocation.equals(written.metadataFileLocation())) {
      refreshFromMetadataLocation(entry.metadataLocation, null, 0, at -> written);
    } else {
      refreshFromMetadataLocation(entry.metadataLocation);
    }
  }

  /**
   * Commits as {@link BaseViewOperations#commit} does, then takes the replica the commit landed in
   * as the view's current state, as a table's operations do (errata D20), so a look at the view
   * after the commit reads nothing: a pointer view holds the metadata it wrote rather than reading
   * it back. If the view cannot be loaded from it, the next {@link #current()} refreshes.
   */
  @Override
  public void commit(ViewMetadata base, ViewMetadata metadata) {
    landed = null;
    sent = null;
    sentLocation = null;
    super.commit(base, metadata);
    Replica<CatalogState> committed = landed;
    ViewMetadata wrote = sentLocation != null
        ? ViewMetadataParser.fromJson(sentLocation, ViewMetadataParser.toJson(sent))
        : null;
    landed = null;
    sent = null;
    sentLocation = null;
    if (committed == null) {
      return;
    }
    try {
      if (committed.state().containsView(view)) {
        loadFrom(committed.state(), wrote);
        lastRead = committed;
      }
    } catch (RuntimeException e) {
      requestRefresh();
    }
  }

  @Override
  protected void doCommit(ViewMetadata base, ViewMetadata metadata) {
    if (base == null) {
      commitCreate(metadata);
      return;
    }
    Replica<CatalogState> read = lastRead;
    if (!holds(read, base)) {
      // Built on metadata other than the version these operations last read: another thread's
      // refresh moved the read on between the check of base and here (as Bug 95 for tables)
      throw new CommitFailedException(
          "Cannot commit %s: stale view metadata (not the version last read)", view);
    }
    CatalogMutation mut = new CatalogMutation(read, createCondition);
    try {
      CatalogState.ViewMetadataRef staged = stage(metadata, mut::wroteAhead);
      mut.updateView(view, staged);
      evictable(mut, staged, metadata);
      landed = mut.commit(log);
    } catch (RuntimeException e) {
      // What it wrote ahead of a record that never takes effect: Iceberg's retry writes anew
      cleanup.delete(mut.abandonedFiles());
      throw e;
    }
  }

  /**
   * Creates the view, or registers it ({@code metadata} names its file), on the read these
   * operations last made, and on a fresh one after a conflict, as a table's create does: its
   * record names its namespace's removals (or version) as read, which a concurrent change there
   * moves. A name another view or a table holds by then fails it ({@link
   * org.apache.iceberg.exceptions.AlreadyExistsException}, Iceberg's messages).
   */
  private void commitCreate(ViewMetadata metadata) {
    Queue<String> written = new ConcurrentLinkedQueue<>();
    CatalogMutation mut = null;
    try {
      CatalogState.ViewMetadataRef staged = stage(metadata, written::add);
      Replica<CatalogState> read = lastRead != null ? lastRead : log.read();
      for (int attempt = 1; ; attempt++) {
        mut = new CatalogMutation(read, createCondition);
        mut.createView(view, staged);
        evictable(mut, staged, metadata);
        try {
          landed = mut.commit(log);
          return;
        } catch (CommitFailedException e) {
          if (attempt >= LogCatalog.OPERATION_ATTEMPTS || !log.waitBeforeRetry(attempt)) {
            if (log.read().state().nameTaken(view)) {
              throw e;
            }
            throw LogCatalog.unavailable(e, attempt, "create view " + view);
          }
          read = log.read();
        }
      }
    } catch (RuntimeException e) {
      // Each attempt's record named the file; only the last may have landed
      if (mut == null || !mut.mayHaveLanded()) {
        cleanup.delete(written);
      }
      throw e;
    }
  }

  /**
   * The view's metadata as a commit sends it: inline if the catalog inlines and it fits, else the
   * location of a metadata file, written now (each file to {@code written}) unless {@code
   * metadata} names one already (a registered view's).
   */
  private CatalogState.ViewMetadataRef stage(ViewMetadata metadata, Consumer<String> written) {
    sent = metadata;
    if (inline && metadata.metadataFileLocation() == null) {
      CatalogState.ViewMetadataRef encoded = InlineViewMetadata.encode(metadata);
      if (encoded.inline().length <= maxUpdateSize) {
        sentLocation = null;
        return encoded;
      }
    }
    String location = writeNewMetadataIfRequired(metadata);
    if (metadata.metadataFileLocation() == null) {
      written.accept(location);
      sentLocation = location;
    } else {
      sentLocation = null; // a registered view's file: read it
    }
    return CatalogState.ViewMetadataRef.location(location);
  }

  /**
   * Makes the inline create or update of {@code metadata} staged on {@code mut} go to a metadata
   * file if its record is larger than the log stores ({@link CatalogMutation#evictLargest}), its
   * file deleted if the record does not land.
   */
  private void evictable(
      CatalogMutation mut, CatalogState.ViewMetadataRef staged, ViewMetadata metadata) {
    if (staged.inline() == null) {
      return;
    }
    mut.viewEvictable(view, new CatalogMutation.Eviction() {
      @Override
      public String write(List<String> out) {
        String location = writeNewMetadataIfRequired(metadata);
        out.add(location);
        sentLocation = location;
        return location;
      }

      @Override
      public void delete(List<String> files) {
        cleanup.delete(files);
      }
    });
  }

  /**
   * Stages the view's create with {@code metadata} on {@code mut}, a catalog transaction's, as a
   * create through these operations sends it (inline, or a metadata file written now, which the
   * transaction deletes if its record does not land): {@link LogCatalog#commitTransaction}.
   */
  void stageCreate(CatalogMutation mut, ViewMetadata metadata) {
    CatalogState.ViewMetadataRef staged = stage(metadata, mut::wroteAhead);
    mut.createView(view, staged);
    evictable(mut, staged, metadata);
  }

  /**
   * Stages the view's update to {@code metadata} on {@code mut}, a catalog transaction's, which
   * applies only on the version the transaction read, as an update through these operations does.
   */
  void stageUpdate(CatalogMutation mut, ViewMetadata metadata) {
    CatalogState.ViewMetadataRef staged = stage(metadata, mut::wroteAhead);
    mut.updateView(view, staged);
    evictable(mut, staged, metadata);
  }

  /**
   * Writes {@code metadata}, which names no file, to a new metadata file of the view's metadata
   * directory, as a commit of a pointer view does, and returns it: for a drop that keeps an inline
   * view's metadata ({@link LogCatalog#dropView}).
   */
  String writeFile(ViewMetadata metadata) {
    return writeNewMetadataIfRequired(metadata);
  }

  /**
   * Whether {@code read} holds {@code base}, the metadata a commit was built on: whether this
   * view loads from {@code base}'s location there (a pointer view's file, or an inline view's
   * location, which names its id and version).
   */
  private boolean holds(Replica<CatalogState> read, ViewMetadata base) {
    String location = base.metadataFileLocation();
    return read != null && location != null && location.equals(location(read.state(), view));
  }

  /** The location {@code view}'s metadata loads from in {@code state}; null if it has no view. */
  static String location(CatalogState state, TableIdentifier view) {
    CatalogState.ViewEntry entry = state.viewEntry(view);
    if (entry == null) {
      return null;
    }
    return entry.isInline()
        ? CatalogState.inlineViewLocation(view, state.viewId(view), entry.version)
        : entry.metadataLocation;
  }
}
