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

import com.kingbase.kes.spark.client.entity.KesReaderPartition
import com.kingbase.kes.spark.client.read.KesReadModeResolver
import com.kingbase.kes.spark.config.{KesConfig, KesOptions}
import com.kingbase.kes.spark.rdd.{AbstractKesRDD, AbstractKesRDDIterator, KesPartition}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.{Partition, SparkContext, TaskContext}

private[spark] class KesRowRDD(sc: SparkContext, params: Map[String, String] = Map.empty,
                                      schema: StructType)
  extends AbstractKesRDD[Row](sc, params) {

  override def compute(split: Partition, context: TaskContext): ScalaKesRowRDDIterator = {
    new ScalaKesRowRDDIterator(context, split.asInstanceOf[KesPartition].kesPartition, schema)
  }
}

private[spark] class ScalaKesRowRDDIterator(context: TaskContext,
                                              partition: KesReaderPartition, schema: StructType)
  extends AbstractKesRDDIterator[Row](context, partition) {

  override def initReader(config: KesConfig): Unit = {
    KesReadModeResolver.resolve(config) match {
      case "thrift" => config.setProperty(KesOptions.KES_VALUE_READER_CLASS, classOf[KesRowThriftReader].getName)
      case "arrow" => config.setProperty(KesOptions.KES_VALUE_READER_CLASS, classOf[KesRowFlightSqlReader].getName)
      case rm: String => throw new IllegalArgumentException("Unknown read mode: " + rm)
    }
  }

  override def createValue(value: Object): Row = {
    value.asInstanceOf[KesRow]
  }
}
