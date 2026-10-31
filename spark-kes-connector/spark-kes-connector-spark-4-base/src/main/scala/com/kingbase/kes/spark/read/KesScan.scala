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

import com.kingbase.kes.spark.config.{KesConfig, KesOptions}
import com.kingbase.kes.spark.util.KesDialects
import org.apache.spark.internal.Logging
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

import scala.language.implicitConversions

class KesScan(config: KesConfig, schema: StructType, filters: Array[Filter]) extends AbstractKesScan(config, schema) with Logging {
  override protected def compiledFilters(): Array[String] = {
    val inValueLengthLimit = config.getValue(KesOptions.KES_FILTER_QUERY_IN_MAX_COUNT)
    filters.map(KesDialects.compileFilter(_, inValueLengthLimit)).filter(_.isDefined).map(_.get)
  }
}
