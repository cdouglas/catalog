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
package org.intentlog;

/**
 * What a {@link SharedLog} can do, which decides how the runtime commits to it.
 *
 * @param append whether it appends at all (an object store without append writes every commit
 *     by {@link SharedLog#replace})
 * @param conditionalAppend whether an append can be conditioned on the tail it expects
 * @param fusedReplace whether it replaces a checkpoint and records in one conditional write
 * @param separateCheckpoints whether it stores checkpoints apart from records ({@link
 *     SharedLog#putCheckpoint}, {@link SharedLog#trim})
 * @param readYourAppends whether a read after an acknowledged append sees it at once
 * @param maxRecordBytes the largest record it stores (0: no limit)
 */
public record Capabilities(
    boolean append,
    boolean conditionalAppend,
    boolean fusedReplace,
    boolean separateCheckpoints,
    boolean readYourAppends,
    long maxRecordBytes) {}
