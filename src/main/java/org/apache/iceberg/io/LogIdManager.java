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
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/**
 * Manages ID allocation and remapping for the log-based catalog format.
 *
 * <p>This class handles:
 *
 * <ul>
 *   <li>Sequential allocation of namespace IDs and table IDs
 *   <li>Late-binding of virtual (negative) IDs to real (positive) IDs
 *   <li>ID remapping for namespaces created within a transaction
 * </ul>
 *
 * <p>Virtual IDs (negative values) are used when creating nested namespaces or tables within a
 * single transaction. When the transaction is applied, virtual IDs are remapped to real sequential
 * IDs.
 */
public class LogIdManager {
  /** Sentinel value indicating an ID needs late-binding. */
  public static final int LATE_BIND = -1;

  private int nextNsid;
  private int nextTblid;
  private final Map<Integer, Integer> nsRemap = Maps.newHashMap();

  /** Creates a new ID manager with default starting values. */
  public LogIdManager() {
    this(1, 1);
  }

  /**
   * Creates a new ID manager with specified starting values.
   *
   * @param nextNsid the next namespace ID to allocate
   * @param nextTblid the next table ID to allocate
   */
  public LogIdManager(int nextNsid, int nextTblid) {
    this.nextNsid = nextNsid;
    this.nextTblid = nextTblid;
  }

  /**
   * Allocates a new namespace ID.
   *
   * @return the allocated namespace ID
   */
  public int allocateNsid() {
    return nextNsid++;
  }

  /**
   * Allocates a new table ID.
   *
   * @return the allocated table ID
   */
  public int allocateTblid() {
    return nextTblid++;
  }

  /**
   * Remaps a virtual (negative) namespace ID to a newly allocated real ID.
   *
   * @param virtualNsid the virtual namespace ID (must be negative)
   * @return the newly allocated real namespace ID
   * @throws IllegalArgumentException if virtualNsid is not negative
   */
  public int remap(int virtualNsid) {
    Preconditions.checkArgument(
        virtualNsid < 0, "Attempting to remap non-virtual namespace: %d", virtualNsid);
    final int assignedNsid = allocateNsid();
    nsRemap.put(virtualNsid, assignedNsid);
    return assignedNsid;
  }

  /**
   * Resolves a namespace ID, returning the remapped real ID for virtual IDs or the original ID for
   * real IDs.
   *
   * @param nsid the namespace ID to resolve
   * @return the resolved namespace ID
   */
  public int resolve(int nsid) {
    if (nsid < 0) {
      Integer remapped = nsRemap.get(nsid);
      Preconditions.checkNotNull(remapped, "Virtual namespace ID not remapped: %d", nsid);
      return remapped;
    }
    return nsid;
  }

  /**
   * Gets the remapped ID for a virtual namespace ID.
   *
   * @param virtualNsid the virtual namespace ID
   * @return the remapped real ID, or null if not remapped
   */
  public Integer getRemapped(int virtualNsid) {
    return nsRemap.get(virtualNsid);
  }

  /**
   * Sets the global ID state from a checkpoint.
   *
   * @param nextNsid the next namespace ID
   * @param nextTblid the next table ID
   */
  public void setGlobals(int nextNsid, int nextTblid) {
    this.nextNsid = nextNsid;
    this.nextTblid = nextTblid;
  }

  public int getNextNsid() {
    return nextNsid;
  }

  public int getNextTblid() {
    return nextTblid;
  }
}
