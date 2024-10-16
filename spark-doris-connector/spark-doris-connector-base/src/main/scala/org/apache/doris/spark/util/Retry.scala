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
        LockSupport.parkNanos(interval.toNanos)
        Success(result)
      case Failure(exception: T) if retryTimes > 0 =>
        logger.warn(s"Execution failed caused by: ", exception)
        logger.warn(s"$retryTimes times retry remaining, the next attempt will be in ${interval.toMillis} ms")
        LockSupport.parkNanos(interval.toNanos)
        h
        exec(retryTimes - 1, interval, logger)(f)(h)
      case Failure(exception) => Failure(exception)
    }
  }

}
