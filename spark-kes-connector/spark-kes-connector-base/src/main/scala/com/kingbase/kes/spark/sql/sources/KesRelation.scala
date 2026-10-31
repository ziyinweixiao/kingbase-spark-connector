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

package com.kingbase.kes.spark.sql.sources

import com.kingbase.kes.spark.client.KesFrontendClient
import com.kingbase.kes.spark.config.{KesConfig, KesOptions}
import com.kingbase.kes.spark.sql.{KesRowRDD, Utils}
import com.kingbase.kes.spark.util.SchemaConvertors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}

import scala.collection.JavaConverters.{asScalaBufferConverter, mapAsJavaMapConverter}
import scala.collection.mutable


private[sql] class KesRelation(
                                  val sqlContext: SQLContext, parameters: Map[String, String])
  extends BaseRelation with TableScan with PrunedScan with PrunedFilteredScan with InsertableRelation {

  private lazy val cfg = KesConfig.fromMap(sqlContext.sparkContext.getConf.getAll.toMap.asJava, parameters.asJava, false)

  private lazy val inValueLengthLimit = cfg.getValue(KesOptions.KES_FILTER_QUERY_IN_MAX_COUNT)

  private lazy val frontend: KesFrontendClient = new KesFrontendClient(cfg)

  private lazy val lazySchema = {
    val tableIdentifier = cfg.getValue(KesOptions.KES_TABLE_IDENTIFIER)
    val tableIdentifierArr = tableIdentifier.split("\\.").map(_.replaceAll("`", ""))
    val kesSchema = frontend.getTableSchema(tableIdentifierArr(0), tableIdentifierArr(1))
    val arrayNativeType = cfg.getValue(KesOptions.KES_READ_ARRAY_NATIVE_TYPE)
    StructType(kesSchema.getProperties.asScala.map(field => {
      StructField(field.getName, SchemaConvertors.toCatalystType(field.getType, field.getPrecision, field.getScale, arrayNativeType))
    }).toArray)

  }

  private lazy val dialect = JdbcDialects.get("")

  override def schema: StructType = lazySchema

  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = {
    filters.filter(Utils.compileFilter(_, dialect, inValueLengthLimit).isEmpty)
  }

  // TableScan
  override def buildScan(): RDD[Row] = buildScan(Array.empty)

  // PrunedScan
  override def buildScan(requiredColumns: Array[String]): RDD[Row] = buildScan(requiredColumns, Array.empty)

  // PrunedFilteredScan
  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val paramWithScan = mutable.LinkedHashMap[String, String]() ++ parameters

    // filter where clause can be handled by Kes BE
    val filterWhereClause: String = {
      filters.flatMap(Utils.compileFilter(_, dialect, inValueLengthLimit))
        .map(filter => s"($filter)").mkString(" and ")
    }

    // required columns for column pruner
    if (requiredColumns != null && requiredColumns.length > 0) {
      paramWithScan += (KesOptions.KES_READ_FIELDS.getName ->
        requiredColumns.map(Utils.quote).mkString(","))
    } else {
      paramWithScan += (KesOptions.KES_READ_FIELDS.getName ->
        lazySchema.fields.map(f => Utils.quote(f.name)).mkString(","))
    }

    if (filters != null && filters.length > 0) {
      paramWithScan += (KesOptions.KES_FILTER_QUERY.getName -> filterWhereClause)
    }

    new KesRowRDD(sqlContext.sparkContext, paramWithScan.toMap, lazySchema)
  }

  // Insert Table
  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    data.write.format("kes")
      .options(cfg.toMap)
      .mode(if (overwrite) SaveMode.Overwrite else SaveMode.Append)
      .save()
  }
}
