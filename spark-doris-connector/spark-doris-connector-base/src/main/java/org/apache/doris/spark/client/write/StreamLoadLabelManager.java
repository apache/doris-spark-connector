// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.spark.client.write;

import org.apache.doris.spark.exception.OptionRequiredException;

import java.io.Serializable;

/**
 * Owns the stream-load label across batches so that a retry of a failed batch can reuse the previous
 * label.
 *
 * <p>An auto-commit (non-2PC) batch that fails on the client side may have actually committed on the
 * backend (e.g. its HTTP response was lost). Reusing the same label on the retry lets the backend
 * reject the retry as a duplicate, so the rows are written exactly once instead of twice. A fresh
 * label is minted for every new (committed) batch, so normal writes are unaffected.
 *
 * <p>Two-phase-commit loads do NOT reuse labels: their exactly-once guarantee comes from the
 * precommit/commit protocol (a retry begins a fresh transaction and the orphaned precommitted one
 * aborts), and a reused-label collision carries no usable transaction id.
 */
class StreamLoadLabelManager implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Backend status returned when a label has already been used by another load. */
    static final String LABEL_ALREADY_EXISTS_STATUS = "Label Already Exists";

    /**
     * {@code ExistingJobStatus} value meaning the load that already used the label has COMMITTED.
     * Other values ("RUNNING", "PRECOMMITTED") mean the original is still in flight and may yet
     * abort, so they must NOT be treated as a successful no-op.
     */
    static final String EXISTING_JOB_FINISHED = "FINISHED";

    /** Generates a fresh label; may fail when a required option is missing. */
    @FunctionalInterface
    interface LabelGenerator {
        String generate() throws OptionRequiredException;
    }

    private final boolean twoPhaseCommitEnabled;
    private volatile String currentLabel;
    private boolean reuseLabel = false;

    StreamLoadLabelManager(boolean twoPhaseCommitEnabled) {
        this.twoPhaseCommitEnabled = twoPhaseCommitEnabled;
    }

    /**
     * Returns the label to use for the next batch: the current label when retrying a failed batch,
     * otherwise a freshly generated one.
     */
    String labelForNextBatch(LabelGenerator generator) throws OptionRequiredException {
        if (!reuseLabel || currentLabel == null) {
            currentLabel = generator.generate();
        }
        return currentLabel;
    }

    /** The label of the most recently started batch (may be {@code null} before the first batch). */
    String currentLabel() {
        return currentLabel;
    }

    /** Whether the current batch is reusing a previous (failed) batch's label. */
    boolean isReusingLabel() {
        return reuseLabel;
    }

    /** The previous batch committed: the next batch must use a fresh label. */
    void onBatchCommitted() {
        reuseLabel = false;
    }

    /**
     * The previous batch failed: reuse the same label on the retry so the backend can deduplicate it.
     * Only applies to auto-commit loads; 2PC loads always get a fresh label.
     */
    void onBatchFailed() {
        if (!twoPhaseCommitEnabled) {
            reuseLabel = true;
        }
    }

    /**
     * Whether a failed-load response means the (reused) label's load already COMMITTED — i.e. the
     * retry is an idempotent no-op and the rows are already present, so it can be treated as success.
     *
     * <p>Requires both that we are retrying with a reused label and that the backend reports the
     * existing job as FINISHED. A still-RUNNING/PRECOMMITTED original may still abort, so it stays a
     * retriable failure rather than being masked as success (which would silently lose those rows).
     */
    static boolean isAlreadyCommitted(boolean labelReused, String status, String existingJobStatus) {
        return labelReused
                && LABEL_ALREADY_EXISTS_STATUS.equalsIgnoreCase(status)
                && EXISTING_JOB_FINISHED.equalsIgnoreCase(existingJobStatus);
    }
}
