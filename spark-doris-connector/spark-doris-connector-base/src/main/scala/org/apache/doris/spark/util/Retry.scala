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

package org.apache.doris.spark.util

import org.slf4j.Logger

import java.time.Duration
import java.util.concurrent.TimeUnit
import scala.annotation.tailrec
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

object Retry {

  @tailrec
  def exec[R, T <: Throwable : ClassTag](retryTimes: Int, interval: Duration, logger: Logger)
                                         (f: => R)(h: => Unit): Try[R] = {
    assert(retryTimes >= 0)
    val result = Try(f)
    result match {
      case Success(result) =>
        Success(result)
      case Failure(exception: T) if retryTimes > 0 =>
        logger.warn("Execution failed caused by: {}", exception.getMessage)
        logger.warn(s"$retryTimes times retry remaining, the next attempt will be in ${interval.toMillis} ms")
        sleepBeforeRetry(interval)
        h
        exec(retryTimes - 1, interval, logger)(f)(h)
      case Failure(exception) => Failure(exception)
    }
  }

  /**
   * Wait for the full interval before the next retry.
   *
   * [[java.util.concurrent.locks.LockSupport.parkNanos]] may return before the requested time on a
   * spurious wakeup or when another thread unparks this one. The stream-load processor interrupts
   * the task thread on an async load failure (to unblock a blocking pipe write), and an interrupt
   * also unparks a parked thread, so a single `parkNanos` for the backoff is repeatedly cut short
   * and the configured retry interval collapses to a few milliseconds — most visibly under Spark
   * 4.x. That both hammers the backend with retries and breaks failover that relies on the interval.
   *
   * Use [[Thread.sleep]] (via [[TimeUnit]]), which is not affected by `unpark`, and loop until the
   * full interval has elapsed. A spurious interrupt is absorbed (the interrupt status is restored
   * afterwards) so the interval is still honored.
   */
  private def sleepBeforeRetry(interval: Duration): Unit = {
    val deadlineNanos = System.nanoTime() + interval.toNanos
    var remainingNanos = interval.toNanos
    var interrupted = false
    while (remainingNanos > 0L) {
      try {
        TimeUnit.NANOSECONDS.sleep(remainingNanos)
      } catch {
        case _: InterruptedException => interrupted = true
      }
      remainingNanos = deadlineNanos - System.nanoTime()
    }
    if (interrupted) Thread.currentThread().interrupt()
  }

}
