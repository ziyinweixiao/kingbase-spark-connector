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

import com.kingbase.kes.spark.config.KesOptions
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object KesSpark {
  def kesRDD(
      sc: SparkContext,
      tableIdentifier: Option[String] = None,
      query: Option[String] = None,
      cfg: Option[Map[String, String]] = None): RDD[AnyRef] = {
    val params = collection.mutable.Map(cfg.getOrElse(Map.empty).toSeq: _*)
    query.map { s => params += (KesOptions.KES_FILTER_QUERY.getName -> s) }
    tableIdentifier.map { s => params += (KesOptions.KES_TABLE_IDENTIFIER.getName -> s) }
    new KesRDD[AnyRef](sc, params.toMap)
  }
}
