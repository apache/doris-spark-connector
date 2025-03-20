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
import java.util.concurrent.locks.LockSupport
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
        LockSupport.parkNanos(interval.toNanos)
        h
        exec(retryTimes - 1, interval, logger)(f)(h)
      case Failure(exception) => Failure(exception)
    }
  }

}
