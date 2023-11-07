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

package org.apache.doris.spark.txn.listener

import org.apache.doris.spark.cfg.SparkSettings
import org.apache.doris.spark.txn.TransactionHandler
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler._
import org.apache.spark.util.CollectionAccumulator

import scala.collection.JavaConverters._
import scala.collection.mutable

class DorisTransactionListener(preCommittedTxnAcc: CollectionAccumulator[Long], settings: SparkSettings)
  extends SparkListener with Logging {

  val txnHandler: TransactionHandler = TransactionHandler(settings)

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    val txnIds: mutable.Buffer[Long] = preCommittedTxnAcc.value.asScala
    jobEnd.jobResult match {
      // if job succeed, commit all transactions
      case JobSucceeded =>
        if (txnIds.isEmpty) {
          log.debug("job run succeed, but there is no pre-committed txn ids")
          return
        }
        log.info("job run succeed, start committing transactions")
        try txnHandler.commitTransactions(txnIds.toList)
        catch {
          case e: Exception => throw e
        }
        finally preCommittedTxnAcc.reset()
        log.info("commit transaction success")
      // if job failed, abort all pre committed transactions
      case _ =>
        if (txnIds.isEmpty) {
          log.debug("job run failed, but there is no pre-committed txn ids")
          return
        }
        log.info("job run failed, start aborting transactions")
        try txnHandler.abortTransactions(txnIds.toList)
        catch {
          case e: Exception => throw e
        }
        finally preCommittedTxnAcc.reset()
        log.info("abort transaction success")
    }
  }

}
