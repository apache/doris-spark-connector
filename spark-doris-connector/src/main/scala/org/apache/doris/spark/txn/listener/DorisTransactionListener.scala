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

import org.apache.doris.spark.load.CommitMessage
import org.apache.doris.spark.txn.TransactionHandler
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler._
import org.apache.spark.util.CollectionAccumulator

import scala.collection.JavaConverters._
import scala.collection.mutable

class DorisTransactionListener(txnAcc: CollectionAccumulator[(String, CommitMessage)], txnHandler: TransactionHandler)
  extends SparkListener with Logging {

  private val jobToStages = mutable.HashMap[Int, List[Int]]()

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    jobToStages += jobStart.jobId -> jobStart.stageIds.toList
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    val stageIds = jobToStages.get(jobEnd.jobId)
    val messages: mutable.Buffer[CommitMessage] = txnAcc.value.asScala.filter(item => {
      stageIds.nonEmpty && stageIds.get.contains(item._1.toInt)
    }).map(_._2)
    jobEnd.jobResult match {
      // if job succeed, commit all transactions
      case JobSucceeded =>
        if (messages.isEmpty) {
          log.debug(s"job ${jobEnd.jobId} run succeed, but there is no pre-committed txn ids")
          return
        }
        log.info(s"job ${jobEnd.jobId} run succeed, start committing transactions")
        try txnHandler.commitTransactions(messages.toList)
        catch {
          case e: Exception => throw e
        }
        finally txnAcc.reset()
        log.info("commit transaction success")
      // if job failed, abort all pre committed transactions
      case _ =>
        if (messages.isEmpty) {
          log.debug(s"job ${jobEnd.jobId} run failed, but there is no pre-committed txn ids")
          return
        }
        log.info(s"job ${jobEnd.jobId} run failed, start aborting transactions")
        try txnHandler.abortTransactions(messages.toList)
        catch {
          case e: Exception => throw e
        }
        finally txnAcc.reset()
        log.info("abort transaction success")
    }
  }

}
