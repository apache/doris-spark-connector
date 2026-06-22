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

import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.concurrent.atomic.AtomicInteger;

public class StreamLoadLabelManagerTest {

    private static final String LABEL_EXISTS = "Label Already Exists";

    /** A deterministic label generator producing label-0, label-1, ... */
    private static StreamLoadLabelManager.LabelGenerator counting(AtomicInteger seq) {
        return () -> "label-" + seq.getAndIncrement();
    }

    /** An auto-commit (non-2PC) manager — the mode that reuses labels. */
    private static StreamLoadLabelManager autoCommit() {
        return new StreamLoadLabelManager(false);
    }

    @Test
    public void firstBatchGeneratesAFreshLabel() throws Exception {
        StreamLoadLabelManager mgr = autoCommit();
        Assertions.assertNull(mgr.currentLabel());
        Assertions.assertFalse(mgr.isReusingLabel());

        String label = mgr.labelForNextBatch(counting(new AtomicInteger()));
        Assertions.assertEquals("label-0", label);
        Assertions.assertEquals("label-0", mgr.currentLabel());
    }

    @Test
    public void committedBatchesEachGetAFreshLabel() throws Exception {
        StreamLoadLabelManager mgr = autoCommit();
        AtomicInteger seq = new AtomicInteger();

        String first = mgr.labelForNextBatch(counting(seq));
        mgr.onBatchCommitted();
        String second = mgr.labelForNextBatch(counting(seq));

        Assertions.assertEquals("label-0", first);
        Assertions.assertEquals("label-1", second);
        Assertions.assertFalse(mgr.isReusingLabel());
    }

    @Test
    public void retryReusesTheFailedBatchLabel() throws Exception {
        StreamLoadLabelManager mgr = autoCommit();
        AtomicInteger seq = new AtomicInteger();

        String first = mgr.labelForNextBatch(counting(seq));
        mgr.onBatchFailed();
        Assertions.assertTrue(mgr.isReusingLabel());

        String retry = mgr.labelForNextBatch(counting(seq));
        Assertions.assertEquals(first, retry, "retry must reuse the failed batch's label");
        Assertions.assertEquals(1, seq.get(), "no new label should be generated on retry");
    }

    @Test
    public void freshLabelResumesAfterASuccessfulRetry() throws Exception {
        StreamLoadLabelManager mgr = autoCommit();
        AtomicInteger seq = new AtomicInteger();

        mgr.labelForNextBatch(counting(seq)); // label-0
        mgr.onBatchFailed();
        mgr.labelForNextBatch(counting(seq)); // reuse label-0
        mgr.onBatchCommitted();               // retry committed
        String next = mgr.labelForNextBatch(counting(seq));

        Assertions.assertEquals("label-1", next);
        Assertions.assertFalse(mgr.isReusingLabel());
    }

    @Test
    public void twoPhaseCommitNeverReusesLabel() throws Exception {
        // Under 2PC, a reused-label collision carries no usable txn id, so labels must stay fresh —
        // exactly-once is handled by the precommit/commit protocol instead.
        StreamLoadLabelManager mgr = new StreamLoadLabelManager(true);
        AtomicInteger seq = new AtomicInteger();

        String first = mgr.labelForNextBatch(counting(seq));
        mgr.onBatchFailed();
        Assertions.assertFalse(mgr.isReusingLabel(), "2PC must not reuse labels");

        String retry = mgr.labelForNextBatch(counting(seq));
        Assertions.assertEquals("label-0", first);
        Assertions.assertEquals("label-1", retry, "2PC retry must get a fresh label");
    }

    @Test
    public void isSerializableWithState() throws Exception {
        // The Spark 2.x/3.x DorisWriter serializes the committer (which holds this manager) to the
        // executors, so the manager must stay Serializable and round-trip its state.
        StreamLoadLabelManager mgr = autoCommit();
        mgr.labelForNextBatch(counting(new AtomicInteger())); // label-0
        mgr.onBatchFailed();

        byte[] bytes;
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(mgr);
            bytes = bos.toByteArray();
        }
        StreamLoadLabelManager restored;
        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
            restored = (StreamLoadLabelManager) ois.readObject();
        }

        Assertions.assertEquals("label-0", restored.currentLabel());
        Assertions.assertTrue(restored.isReusingLabel());
    }

    @Test
    public void alreadyCommittedOnlyForReusedLabelThatActuallyFinished() {
        // The exactly-once no-op: a reused label whose existing load has COMMITTED (FINISHED).
        Assertions.assertTrue(StreamLoadLabelManager.isAlreadyCommitted(true, LABEL_EXISTS, "FINISHED"));
        Assertions.assertTrue(StreamLoadLabelManager.isAlreadyCommitted(true, "label already exists", "finished"));

        // A still-in-flight original may yet abort — must NOT be masked as success (would lose data).
        Assertions.assertFalse(StreamLoadLabelManager.isAlreadyCommitted(true, LABEL_EXISTS, "RUNNING"));
        Assertions.assertFalse(StreamLoadLabelManager.isAlreadyCommitted(true, LABEL_EXISTS, "PRECOMMITTED"));
        Assertions.assertFalse(StreamLoadLabelManager.isAlreadyCommitted(true, LABEL_EXISTS, null));

        // A fresh-label collision is a real error, not a dedup no-op.
        Assertions.assertFalse(StreamLoadLabelManager.isAlreadyCommitted(false, LABEL_EXISTS, "FINISHED"));
        // Any other failure status is a real error even on a reused label.
        Assertions.assertFalse(StreamLoadLabelManager.isAlreadyCommitted(true, "Fail", "FINISHED"));
        Assertions.assertFalse(StreamLoadLabelManager.isAlreadyCommitted(true, null, "FINISHED"));
    }
}
