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

package com.kingbase.kes.spark.sql

import com.kingbase.kes.spark.client.KesFrontendClient
import com.kingbase.kes.spark.config.{KesConfig, KesOptions}
import com.kingbase.kes.spark.load.CommitMessage
import com.kingbase.kes.spark.sql.KesSourceProvider.SHORT_NAME
import com.kingbase.kes.spark.sql.sources.{KesRelation, KesSourceRegisterTrait}
import com.kingbase.kes.spark.writer.KesWriter
import com.kingbase.kes.spark.writer.S3TvfBatchWriter
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.slf4j.{Logger, LoggerFactory}

import java.sql.Connection
import java.util.function.Function
import scala.collection.JavaConverters.mapAsJavaMapConverter

private[sql] class KesSourceProvider extends KesSourceRegisterTrait
  with RelationProvider
  with CreatableRelationProvider
  with StreamSinkProvider
  with Serializable {

  private val logger: Logger = LoggerFactory.getLogger(classOf[KesSourceProvider].getName)

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    new KesRelation(sqlContext, parameters)
  }


  /**
   * df.save
   */
  override def createRelation(sqlContext: SQLContext,
                              mode: SaveMode, parameters: Map[String, String],
                              data: DataFrame): BaseRelation = {

    val config = KesConfig.fromMap(sqlContext.sparkContext.getConf.getAll.toMap.asJava, parameters.asJava, false)

    mode match {
      case SaveMode.Overwrite =>
        truncateTable(config)
      case _: SaveMode => // do nothing
    }

    if (config.getValue(KesOptions.LOAD_MODE) == "tvf") {
      new S3TvfBatchWriter(config).write(data)
    } else {
      // accumulator for transaction handling
      val acc = sqlContext.sparkContext.collectionAccumulator[CommitMessage]("BatchTxnAcc")
      // init stream loader
      val writer = new KesWriter(config, acc, false)
      writer.write(data)
    }

    new BaseRelation {
      override def sqlContext: SQLContext = unsupportedException

      override def schema: StructType = unsupportedException

      override def needConversion: Boolean = unsupportedException

      override def sizeInBytes: Long = unsupportedException

      override def unhandledFilters(filters: Array[Filter]): Array[Filter] = unsupportedException

      private def unsupportedException =
        throw new UnsupportedOperationException("BaseRelation from kes write operation is not usable.")
    }
  }

  override def createSink(sqlContext: SQLContext, parameters: Map[String, String], partitionColumns: Seq[String], outputMode: OutputMode): Sink = {
    val config = KesConfig.fromMap(sqlContext.sparkContext.getConf.getAll.toMap.asJava,
      parameters.asJava, false)
    if (config.getValue(KesOptions.LOAD_MODE) == "tvf") {
      throw new UnsupportedOperationException("tvf write mode does not support Structured Streaming")
    }
    new KesStreamLoadSink(sqlContext, config)
  }

  private def truncateTable(config: KesConfig): Unit = {
    val frontend = new KesFrontendClient(config)
    val queryFunc: Function[Connection, Void] = new Function[Connection, Void]() {
      override def apply(conn: Connection): Void = {
        val query = s"TRUNCATE TABLE ${config.getValue(KesOptions.KES_TABLE_IDENTIFIER)}"
        val stmt = conn.createStatement()
        stmt.execute(query)
        logger.info(s"truncate table ${config.getValue(KesOptions.KES_TABLE_IDENTIFIER)} success")
        null
      }
    }
    frontend.queryFrontends(queryFunc)
  }

}

object KesSourceProvider {
  val SHORT_NAME: String = "kes"
}
