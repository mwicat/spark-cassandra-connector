package com.datastax.spark.connector.cql

import scala.collection.JavaConversions._
import scala.language.existentials

import com.datastax.driver.core.policies.RetryPolicy
import com.datastax.driver.core.policies.RetryPolicy.RetryDecision
import com.datastax.driver.core.{ConsistencyLevel, Statement, WriteType}

import org.apache.spark.Logging

import com.datastax.driver.core._
import com.datastax.spark.connector._

/** Always retries with the same CL, constant number of times, regardless of circumstances */
class MultipleRetryPolicy(maxRetryCount: Int, retryDelay: CassandraConnectorConf.RetryDelayConf)
  extends RetryPolicy with Logging {

  private def retryOrThrow(cl: ConsistencyLevel, nbRetry: Int): RetryDecision = {
    if (nbRetry < maxRetryCount) {
      if (nbRetry > 0) {
        val delay = retryDelay.forRetry(nbRetry).toMillis
        if (delay > 0) Thread.sleep(delay)
      }
      RetryDecision.retry(cl)
    } else {
      RetryDecision.rethrow()
    }
  }

  override def onReadTimeout(stmt: Statement, cl: ConsistencyLevel,
                             requiredResponses: Int, receivedResponses: Int,
                             dataRetrieved: Boolean, nbRetry: Int) = {
    if (nbRetry >= maxRetryCount) {
      stmt match {
        case bs: BoundStatement => {
          val values = bs.preparedStatement().getVariables().zipWithIndex map { case (defn, i) =>
            val rawValue = bs.getBytesUnsafe(i)
            if (rawValue == null) null else defn.getType().deserialize(rawValue, ProtocolVersion.V3)
          }
          logError(s"Timeout statement $nbRetry " + bs.preparedStatement().getQueryString() + "[" + values.mkString(",") + "]")
        }
        case unknown => Unit
      }
    }

    retryOrThrow(cl, nbRetry)
  }

  override def onUnavailable(stmt: Statement, cl: ConsistencyLevel,
                             requiredReplica: Int, aliveReplica: Int, nbRetry: Int) = retryOrThrow(cl, nbRetry)

  override def onWriteTimeout(stmt: Statement, cl: ConsistencyLevel, writeType: WriteType,
                              requiredAcks: Int, receivedAcks: Int, nbRetry: Int) = retryOrThrow(cl, nbRetry)

}
