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

import com.kingbase.kes.spark.config.KesConfig
import com.kingbase.kes.spark.read.KesScanBuilder
import com.kingbase.kes.spark.write.KesWriteBuilder
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.WriteBuilder
import org.apache.spark.sql.types.StructType

class KesTable(identifier: Identifier, config: KesConfig, schema: Option[StructType])
  extends KesTableBase(identifier, config, schema) {

  override def createScanBuilder(config: KesConfig, schema: StructType): ScanBuilder = new KesScanBuilder(config, schema)
  override protected def createWriteBuilder(config: KesConfig, schema: StructType): WriteBuilder = new KesWriteBuilder(config, schema)

}
