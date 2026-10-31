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

package com.kingbase.kes.spark.catalog

import com.kingbase.kes.spark.client.KesFrontendClient
import com.kingbase.kes.spark.config.{KesConfig, KesOptions}
import com.kingbase.kes.spark.rest.models.Schema
import com.kingbase.kes.spark.util.SchemaConvertors
import org.apache.spark.sql.connector.catalog.TableCapability._
import org.apache.spark.sql.connector.catalog.{Identifier, SupportsRead, SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import scala.collection.JavaConverters._
import scala.collection.mutable


abstract class KesTableBase(identifier: Identifier, config: KesConfig, schema: Option[StructType]) extends Table with SupportsRead with SupportsWrite {

  private lazy val frontend:KesFrontendClient = new KesFrontendClient(config)

  override def name(): String = identifier.toString

  override def schema(): StructType = schema.getOrElse({
    val kesSchema = frontend.getTableSchema(identifier.namespace()(0), identifier.name())
    kesSchemaToStructType(kesSchema)
  })

  override def capabilities(): util.Set[TableCapability] = {
    val capabilities = mutable.Set(BATCH_READ,
      BATCH_WRITE,
      STREAMING_WRITE,
      TRUNCATE)
    val properties = config.getSinkProperties
    val partialColumnsEnabled = properties.containsKey(KesOptions.PARTIAL_COLUMNS) && "true".equalsIgnoreCase(properties.get(KesOptions.PARTIAL_COLUMNS))
    val schemaLessEnabled = config.getValue(KesOptions.KES_WRITE_SCHEMA_LESS)
    if (partialColumnsEnabled || schemaLessEnabled) {
      capabilities += ACCEPT_ANY_SCHEMA
    }
    capabilities.asJava
  }

  override def newScanBuilder(caseInsensitiveStringMap: CaseInsensitiveStringMap): ScanBuilder = {
    config.setProperty(KesOptions.KES_TABLE_IDENTIFIER, name())
    createScanBuilder(config, schema())
  }

  override def newWriteBuilder(logicalWriteInfo: LogicalWriteInfo): WriteBuilder = {
    config.setProperty(KesOptions.KES_TABLE_IDENTIFIER, name())
    createWriteBuilder(config, logicalWriteInfo.schema())
  }

  private def kesSchemaToStructType(kesSchema: Schema): StructType = {
    val arrayNativeType = config.getValue(KesOptions.KES_READ_ARRAY_NATIVE_TYPE)
    StructType(kesSchema.getProperties.asScala.map(field => {
      StructField(field.getName, SchemaConvertors.toCatalystType(field.getType, field.getPrecision, field.getScale, arrayNativeType))
    }).toArray)
  }

  protected def createScanBuilder(config: KesConfig, schema: StructType): ScanBuilder

  protected def createWriteBuilder(config: KesConfig, schema: StructType): WriteBuilder

}
