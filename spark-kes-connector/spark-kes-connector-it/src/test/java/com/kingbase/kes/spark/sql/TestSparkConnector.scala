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

import com.kingbase.kes.spark.config.{KesConfig, KesOptions}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{Ignore, Test}

import java.util

// This test need real connect info to run.
// Set the connect info before comment out this @Ignore
@Ignore
class TestSparkConnector {
  val kesFeNodes = "your_fe_host:8030"
  val kesUser = "root"
  val kesPwd = ""
  val kesTable = "test.test_tbl"

  val kafkaServers = ""
  val kafkaTopics = ""

  @Test
  def rddReadTest(): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    import com.kingbase.kes.spark._
    val kesSparkRDD = sc.kesRDD(
      tableIdentifier = Some(kesTable),
      cfg = Some(Map(
        "kes.fenodes" -> kesFeNodes,
        "kes.request.auth.user" -> kesUser,
        "kes.request.auth.password" -> kesPwd
      ))
    )
    kesSparkRDD.map(println(_)).count()
    sc.stop()
  }

  @Test
  def dataframeWriteTest(): Unit = {
    val session = SparkSession.builder().master("local[*]").getOrCreate()
    val df = session.createDataFrame(Seq(
      ("zhangsan", "m"),
      ("lisi", "f"),
      ("wangwu", "m")
    )).toDF("name", "gender")
    df.write
      .format("kes")
      .option("kes.fenodes", kesFeNodes)
      .option("kes.table.identifier", kesTable)
      .option("kes.user", kesUser)
      .option("kes.password", kesPwd)
      //      .option("sink.auto-redirect", "true")
      //specify your field
      .option("kes.write.fields", "name,gender")
      .option("sink.batch.size", 2)
      .option("sink.max-retries", 2)
      .save()
    session.stop()
  }

  @Test
  def dataframeReadTest(): Unit = {
    val session = SparkSession.builder().master("local[*]").getOrCreate()
    val kesSparkDF = session.read
      .format("kes")
      .option("kes.fenodes", kesFeNodes)
      .option("kes.table.identifier", kesTable)
      .option("kes.user", kesUser)
      .option("kes.password", kesPwd)
      .load()

    kesSparkDF.show()
    session.stop()
  }


  @Test
  def structuredStreamingWriteTest(): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .getOrCreate()
    val df = spark.readStream
      .option("kafka.bootstrap.servers", kafkaServers)
      .option("startingOffsets", "latest")
      .option("subscribe", kafkaTopics)
      .format("kafka")
      .option("failOnDataLoss", false)
      .load()

    df.selectExpr("CAST(timestamp AS STRING)", "CAST(partition as STRING)")
      .writeStream
      .format("kes")
      .option("checkpointLocation", "/tmp/test")
      .option("kes.table.identifier", kesTable)
      .option("kes.fenodes", kesFeNodes)
      .option("kes.user", kesUser)
      .option("kes.password", kesPwd)
      .option("sink.batch.size", 2)
      .option("sink.max-retries", 2)
      .start().awaitTermination()
    spark.stop()
  }

  @Test
  def kesConfigCheckTest(): Unit = {
    val map = new util.HashMap[String, String]()
    map.put(KesOptions.KES_TABLE_IDENTIFIER.getName, "db.table")
    map.put(KesOptions.KES_USER.getName, "user")
    map.put(KesOptions.KES_PASSWORD.getName, "pwd")
    for (url <- List("fe1:8080,fe2:8120", "nginx/test")) {
      map.put(KesOptions.KES_FENODES.getName, url)
      KesConfig.fromMap(map, false)
    }

    for (url <- List("fe1:8080,fe2:8120,", "fe1:8080;fe2:8120")) {
      var exRaised = false
      try {
        map.put(KesOptions.KES_FENODES.getName, url)
        KesConfig.fromMap(map, false)
      } catch {
        case _:IllegalArgumentException =>
          exRaised = true
      }
      assert(exRaised)
    }
  }
}

