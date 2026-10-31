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

package com.kingbase.kes.spark.write

import com.kingbase.kes.spark.client.write.{CopyIntoProcessor, KesCommitter, KesWriter, StreamLoadProcessor}
import com.kingbase.kes.spark.config.{KesConfig, KesOptions}
import com.kingbase.kes.spark.util.Retry
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

import java.time.Duration
import java.util.concurrent.locks.LockSupport
import scala.collection.mutable
import scala.util.{Failure, Success}

class KesDataWriter(config: KesConfig, schema: StructType, partitionId: Int, taskId: Long, epochId: Long = -1) extends DataWriter[InternalRow] with Logging {

  private val (writer: KesWriter[InternalRow], committer: KesCommitter) =
    config.getValue(KesOptions.LOAD_MODE) match {
      case "stream_load" => (new StreamLoadProcessor(config, schema), new StreamLoadProcessor(config, schema))
      case "copy_into" => (new CopyIntoProcessor(config, schema), new CopyIntoProcessor(config, schema))
      case mode => throw new IllegalArgumentException("Unsupported load mode: " + mode)
    }

  private val batchIntervalMs = config.getValue(KesOptions.KES_SINK_BATCH_INTERVAL_MS)

  private val retries = config.getValue(KesOptions.KES_SINK_MAX_RETRIES)

  private val retryIntervalMs = config.getValue(KesOptions.KES_SINK_RETRY_INTERVAL_MS)

  private val twoPhaseCommitEnabled = config.getValue(KesOptions.KES_SINK_ENABLE_2PC)

  private val committedMessages = mutable.Buffer[String]()

  private lazy val recordBuffer = mutable.Buffer[InternalRow]()

  override def write(record: InternalRow): Unit = loadBatchWithRetries(record)

  override def commit(): WriterCommitMessage = {
    val txnId = Option(writer.stop())
    if (twoPhaseCommitEnabled) {
      if (txnId.isDefined) {
        committedMessages += txnId.get
      } else {
        log.warn("No txn {} to commit batch", txnId)
      }
    }
    KesWriterCommitMessage(partitionId, taskId, epochId, committedMessages.toArray)
  }

  override def abort(): Unit = {
    if (committedMessages.nonEmpty) {
      committedMessages.foreach(msg => committer.abort(msg))
    }
    close()
  }

  override def close(): Unit = {
    if (writer != null) {
      writer.close()
    }
  }

  @throws[Exception]
  private def loadBatchWithRetries(record: InternalRow): Unit = {
    var isRetrying = false
    Retry.exec[Unit, Exception](retries, Duration.ofMillis(retryIntervalMs.toLong), log) {
      if (isRetrying) {
        // retrying, reload data from buffer
        while (writer.getBatchCount < recordBuffer.size){
          val idx = writer.getBatchCount
          writer.load(recordBuffer(idx))
        }
        isRetrying = false
      }
      if (writer.endOfBatch()) {
        // end of batch, stop batch write
        val txnId = Option(writer.stop())
        if (twoPhaseCommitEnabled) {
          if (txnId.isDefined) {
            committedMessages += txnId.get
          } else {
            throw new Exception("Failed to end batch write")
          }
        }
        // clear buffer if retry is enabled
        if (retries > 0) {
          recordBuffer.clear()
        }
        writer.resetBatchCount()
        LockSupport.parkNanos(Duration.ofMillis(batchIntervalMs.toLong).toNanos)
      }
      writer.load(record)
    } {
      // batch write failed, set retry flag and reset batch count
      isRetrying = true
      writer.resetBatchCount()
    } match {
      case Success(_) => if (retries > 0) recordBuffer += record
      case Failure(exception) => throw new Exception(exception)
    }
  }

}

case class KesWriterCommitMessage(partitionId: Int, taskId: Long, epochId: Long, commitMessages: Array[String]) extends WriterCommitMessage
