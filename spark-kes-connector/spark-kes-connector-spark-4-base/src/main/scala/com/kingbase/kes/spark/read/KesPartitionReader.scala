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

package com.kingbase.kes.spark.read

import com.kingbase.kes.spark.client.entity.KesReaderPartition
import com.kingbase.kes.spark.client.read.{KesFlightSqlReader, KesReader, KesThriftReader}
import com.kingbase.kes.spark.config.KesConfig
import com.kingbase.kes.spark.util.RowConvertors
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader}
import org.apache.spark.sql.types.StructType

import scala.language.implicitConversions

class KesPartitionReader(inputPartition: InputPartition, schema: StructType, mode: ScanMode, config: KesConfig)
  extends PartitionReader[InternalRow] {

  private implicit def toReaderPartition(inputPart: KesInputPartition): KesReaderPartition = {
    val tablets = inputPart.tablets.map(java.lang.Long.valueOf)
    new KesReaderPartition(inputPart.database, inputPart.table, inputPart.backend, tablets,
      inputPart.opaquedQueryPlan, inputPart.readCols, inputPart.predicates, inputPart.limit, config, inputPart.datetimeJava8ApiEnabled)
  }

  private lazy val reader: KesReader = {
    mode match {
      case ScanMode.THRIFT => new KesThriftReader(inputPartition.asInstanceOf[KesInputPartition])
      case ScanMode.ARROW => new KesFlightSqlReader(inputPartition.asInstanceOf[KesInputPartition])
      case _ => throw new UnsupportedOperationException()
    }
  }

  private val datetimeJava8ApiEnabled: Boolean = inputPartition.asInstanceOf[KesInputPartition].datetimeJava8ApiEnabled

  override def next(): Boolean = reader.hasNext

  override def get(): InternalRow = {
    val values = reader.next().asInstanceOf[Array[Any]]
    val row = new GenericInternalRow(schema.length)
    if (values.nonEmpty) {
      values.zipWithIndex.foreach {
        case (value, index) =>
          if (value == null) row.setNullAt(index)
          else {
            val dataType = schema.fields(index).dataType
            val catalystValue = RowConvertors.convertValue(value, dataType, datetimeJava8ApiEnabled)
            row.update(index, catalystValue)
          }
      }
    }
    row
  }

  override def close(): Unit = {
    if (reader != null) {
      reader.close()
    }
  }

}
