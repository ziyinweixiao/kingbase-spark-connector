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

package com.kingbase.kes.spark.rdd

import com.kingbase.kes.spark.client.entity.KesReaderPartition
import com.kingbase.kes.spark.client.read.KesReader
import com.kingbase.kes.spark.config.{KesConfig, KesOptions}
import org.apache.spark.util.TaskCompletionListener
import org.apache.spark.{TaskContext, TaskKilledException}
import org.slf4j.{Logger, LoggerFactory}

private[spark] abstract class AbstractKesRDDIterator[T](
    context: TaskContext,
    partition: KesReaderPartition) extends Iterator[T] {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass.getName.stripSuffix("$"))
  private var initialized = false
  private var closed = false

  // the reader obtain data from Kes BE
  private lazy val reader = {
    initialized = true
    val config = partition.getConfig
    initReader(config)
    val valueReaderName = config.getValue(KesOptions.KES_VALUE_READER_CLASS)
    logger.debug(s"Use value reader '$valueReaderName'.")
    val cons = Class.forName(valueReaderName).getDeclaredConstructor(classOf[KesReaderPartition])
    cons.newInstance(partition).asInstanceOf[KesReader]
  }

  context.addTaskCompletionListener(new TaskCompletionListener() {
    override def onTaskCompletion(context: TaskContext): Unit = {
      closeIfNeeded()
    }
  })

  override def hasNext: Boolean = {
    if (context.isInterrupted()) {
      throw new TaskKilledException
    }
    reader.hasNext
  }

  override def next(): T = {
    if (!hasNext) {
      throw new NoSuchElementException("End of stream")
    }
    val value = reader.next()
    createValue(value)
  }

  private def closeIfNeeded(): Unit = {
    logger.trace(s"Close status is '$closed' when close Kes RDD Iterator")
    if (!closed) {
      close()
      closed = true
    }
  }

  protected def close(): Unit = {
    logger.trace(s"Initialize status is '$initialized' when close Kes RDD Iterator")
    if (initialized) {
      reader.close()
    }
  }

  def initReader(config: KesConfig): Unit

  /**
   * convert value of row from reader.next return type to T.
   * @param value reader.next return value
   * @return value of type T
   */
  def createValue(value: Object): T
}
