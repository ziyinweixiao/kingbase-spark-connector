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
import com.kingbase.kes.spark.client.read.KesFlightSqlReader
import com.kingbase.kes.spark.config.KesOptions
import com.kingbase.kes.spark.exception.ShouldNeverHappenException

import scala.collection.JavaConverters.{asScalaBufferConverter, mapAsScalaMapConverter}

class KesRowFlightSqlReader(partition: KesReaderPartition) extends KesFlightSqlReader(partition)  {

  private val rowOrder: Seq[String] = config.getValue(KesOptions.KES_READ_FIELDS).split(",")

  override def next(): AnyRef = {
    if (!hasNext) {
      throw new ShouldNeverHappenException
    }
    val row: KesRow = new KesRow(rowOrder)
    rowBatch.next.asScala.zipWithIndex.foreach {
      case (s, index) if index < row.values.size && s.isInstanceOf[java.util.HashMap[String, String]] =>
        row.values.update(index, s.asInstanceOf[java.util.HashMap[String, String]].asScala)
      case (s, index) if index < row.values.size && s.isInstanceOf[java.util.List[_]] =>
        row.values.update(index, s.asInstanceOf[java.util.List[_]].asScala)
      case (s, index) if index < row.values.size => row.values.update(index, s)
      case _ => // nothing
    }
    row
  }
}
