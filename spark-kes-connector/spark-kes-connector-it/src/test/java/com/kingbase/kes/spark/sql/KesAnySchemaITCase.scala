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

import org.apache.commons.lang3.StringUtils
import com.kingbase.kes.spark.container.{AbstractContainerTestBase, ContainerUtils}
import com.kingbase.kes.spark.container.AbstractContainerTestBase.{assertEqualsInAnyOrder, getKesQueryConnection}
import com.kingbase.kes.spark.rest.models.DataModel
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.junit.Test
import org.slf4j.LoggerFactory

import java.util
import scala.collection.JavaConverters._

class KesAnySchemaITCase extends AbstractContainerTestBase {

  private val LOG = LoggerFactory.getLogger(classOf[KesAnySchemaITCase])

  val DATABASE: String = "example_db"
  /*
   *   CREATE TABLE table4
   *   (
   *       siteid INT DEFAULT '10',
   *       citycode SMALLINT,
   *       username VARCHAR(32) DEFAULT '',
   *       pv BIGINT DEFAULT '0'
   *   )
   *   UNIQUE KEY(siteid, citycode, username)
   *   DISTRIBUTED BY HASH(siteid) BUCKETS 10
   *   PROPERTIES("replication_num" = "1");
   */
  val kesTable = "table4"

  /*
   *   CREATE TABLE table2
   *   (
   *       siteid INT DEFAULT '10',
   *       citycode SMALLINT,
   *       username VARCHAR(32) DEFAULT '',
   *       pv BIGINT DEFAULT '0'
   *   )
   *   UNIQUE KEY(siteid, citycode, username)
   *   DISTRIBUTED BY HASH(siteid) BUCKETS 10
   *   PROPERTIES("replication_num" = "1");
   */
  val kesSourceTable = "table2"

  /*
   *   CREATE TABLE table5
   *   (
   *       siteid INT DEFAULT '10',
   *       citycode SMALLINT,
   *       username VARCHAR(32) DEFAULT '',
   *       pv BIGINT DEFAULT '0',
   *       p_value BIGINT DEFAULT '0'
   *   )
   *   UNIQUE KEY(siteid, citycode, username)
   *   DISTRIBUTED BY HASH(siteid) BUCKETS 10
   *   PROPERTIES("replication_num" = "1");
   */
  val kesPartialTable = "table5"

  val kesSchemaLessTable = "table_schema_less"

  @Test
  def jsonDataWriteTest(): Unit = {
    initializeTable(kesTable, DataModel.UNIQUE)

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    try {
      val df = spark.createDataFrame(Seq(
        (0, 0, "user1", 100),
        (1, 0, "user2", 100),
        (3, 0, "user2", 200)
      )).toDF("siteid", "citycode", "username", "pv")
      df.write
        .format("kes")
        .option("kes.fenodes", getFenodes)
        .option("kes.table.identifier", DATABASE + "." + kesTable)
        .option("kes.user", getKesUsername)
        .option("kes.password", getKesPassword)
        .option("kes.sink.properties.format", "json")
        .option("kes.sink.properties.compress_type", "")
        .option("sink.batch.size", 2)
        .option("sink.max-retries", 2)
        .mode(SaveMode.Append)
        .save()

      Thread.sleep(10000)
      val actual = ContainerUtils.executeSQLStatement(
        getKesQueryConnection,
        LOG,
        String.format("select * from %s.%s", DATABASE, kesTable),
        4)
      val expected = util.Arrays.asList("0,0,user1,100", "1,0,user2,100", "3,0,user2,200");
      checkResultInAnyOrder("jsonDataWriteTest", expected.toArray, actual.toArray)
    } finally {
      spark.stop()
    }
  }

  @Test
  def jsonDataWriteWithPartialUpdateTest(): Unit = {
    initializePartialTable(kesPartialTable, DataModel.UNIQUE)
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    try {
      val df = spark.createDataFrame(Seq(
        (0, 0, "user4", 100),
        (1, 0, "user5", 100),
        (3, 0, "user6", 200)
      )).toDF("siteid", "citycode", "username", "pv")
      df.write
        .format("kes")
        .option("kes.fenodes", getFenodes)
        .option("kes.table.identifier", DATABASE + "." + kesPartialTable)
        .option("kes.user", getKesUsername)
        .option("kes.password", getKesPassword)
        .option("kes.sink.properties.format", "json")
        .option("kes.sink.properties.compress_type", "")
        .option("kes.sink.properties.partial_columns", "true")
        .option("kes.write.fields", "siteid,citycode,username,pv")
        .option("sink.batch.size", 2)
        .option("sink.max-retries", 2)
        .mode(SaveMode.Append)
        .save()
      spark.stop()
      Thread.sleep(2000)
      val actual = ContainerUtils.executeSQLStatement(
        getKesQueryConnection,
        LOG,
        String.format("select * from %s.%s", DATABASE, kesPartialTable),
        5)
      val expected = util.Arrays.asList("0,0,user4,100,0", "1,0,user5,100,0", "3,0,user6,200,0");
      checkResultInAnyOrder("jsonDataWriteWithPartialUpdateTest", expected.toArray, actual.toArray)
    } finally {
      spark.stop()
    }
  }

  @Test
  def jsonDataWriteSqlTest(): Unit = {
    initializeTable(kesTable, DataModel.UNIQUE)
    initializeTable(kesSourceTable, DataModel.UNIQUE)
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    try {
      val kes = spark.sql(
        s"""
           |CREATE TEMPORARY VIEW test_lh
           |USING kes
           |OPTIONS(
           | "table.identifier"="${DATABASE + "." + kesTable}",
           | "fenodes"="${getFenodes}",
           | "user"="${getKesUsername}",
           | "password"="${getKesPassword}"
           |)
           |""".stripMargin)
      spark.sql(
        """
          |insert into test_lh values (0, 0, "user1", 100), (1, 0, "user2", 100),(2, 1, "user3", 100),(3, 0, "user2", 200)
          |""".stripMargin)

      spark.sql(
        s"""
           |CREATE TEMPORARY VIEW table2
           |USING kes
           |OPTIONS(
           | "table.identifier"="${DATABASE + "." + kesSourceTable}",
           | "fenodes"="${getFenodes}",
           | "user"="${getKesUsername}",
           | "password"="${getKesPassword}"
           |)
           |""".stripMargin)

      spark.sql(
        """
          |insert into table2 values (0, 0, "user1", 100), (1, 0, "user2", 100),(2, 1, "user3", 100),(3, 0, "user2", 200)
          |""".stripMargin)

      spark.sql(
        """
          |insert into test_lh select siteid, citycode + 1, username, pv + 1 from table2
          |""".stripMargin)

      Thread.sleep(2000)
      val actual = ContainerUtils.executeSQLStatement(
        getKesQueryConnection,
        LOG,
        String.format("select * from %s.%s", DATABASE, kesTable),
        4)
      val expected = util.Arrays.asList("0,0,user1,100", "1,0,user2,100", "2,1,user3,100", "3,0,user2,200",
        "0,1,user1,101", "1,1,user2,101", "2,2,user3,101", "3,1,user2,201");
      checkResultInAnyOrder("jsonDataWriteSqlTest", expected.toArray, actual.toArray)
    } finally {
      spark.stop()
    }
  }

  @Test
  def jsonDataWriteWithPartialUpdateSqlTest(): Unit = {
    initializePartialTable(kesPartialTable, DataModel.UNIQUE)
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    try {
      val version = spark.version
      LOG.info("spark version: " + version)
      if (StringUtils.startsWith(version, "2")
        || StringUtils.startsWith(version, "3.1")
        || StringUtils.startsWith(version, "3.2")
        || StringUtils.startsWith(version, "3.4")) {
        LOG.warn("sql partial_columns is only support in spark3.3/3.5+")
        return
      }
      val kes = spark.sql(
        s"""
           |CREATE TEMPORARY VIEW test_lh
           |USING kes
           |OPTIONS(
           | "table.identifier"="${DATABASE + "." + kesPartialTable}",
           | "fenodes"="${getFenodes}",
           | "user"="${getKesUsername}",
           | "password"="${getKesPassword}",
           | "kes.sink.properties.format" = "csv",
           | "kes.sink.properties.partial_columns" = "true",
           | "kes.write.fields" = "siteid,citycode,username,pv"
           |)
           |""".stripMargin)
      spark.sql(
        """
          |INSERT INTO test_lh (siteid, citycode, username, pv) VALUES (0, 0, 'user1',3)
          |""".stripMargin)

      Thread.sleep(2000)
      val actual = ContainerUtils.executeSQLStatement(
        getKesQueryConnection,
        LOG,
        String.format("select * from %s.%s", DATABASE, kesPartialTable),
        5)
      val expected = util.Arrays.asList("0,0,user1,3,0");
      checkResultInAnyOrder("jsonDataWriteWithPartialUpdateSqlTest", expected.toArray, actual.toArray)
    } finally {
      spark.stop()
    }
  }

  @Test
  def jsonDataWriteWithPartialUpdateSqlTest1(): Unit = {
    initializePartialTable(kesPartialTable, DataModel.UNIQUE)
    initializeTable(kesSourceTable, DataModel.UNIQUE)
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    try {
      val version = spark.version
      LOG.info("spark version: " + version)
      if (StringUtils.startsWith(version, "2")
        || StringUtils.startsWith(version, "3.1")
        || StringUtils.startsWith(version, "3.2")
        || StringUtils.startsWith(version, "3.4")) {
        LOG.warn("sql partial_columns is only support in spark3.3/3.5+")
        return
      }
      val kes = spark.sql(
        s"""
           |CREATE TEMPORARY VIEW test_lh
           |USING kes
           |OPTIONS(
           | "table.identifier"="${DATABASE + "." + kesPartialTable}",
           | "fenodes"="${getFenodes}",
           | "user"="${getKesUsername}",
           | "password"="${getKesPassword}",
           | "kes.sink.properties.format" = "json",
           | "kes.sink.properties.compress_type" = "",
           | "kes.sink.properties.partial_columns" = "true"
           |)
           |""".stripMargin)

      spark.sql(
        s"""
           |CREATE TEMPORARY VIEW table2
           |USING kes
           |OPTIONS(
           | "table.identifier"="${DATABASE + "." + kesSourceTable}",
           | "fenodes"="${getFenodes}",
           | "user"="${getKesUsername}",
           | "password"="${getKesPassword}"
           |)
           |""".stripMargin)

      spark.sql(
        """
          |INSERT INTO table2 (siteid, citycode, username, pv) VALUES (0, 0, 'user1',3)
          |""".stripMargin)

      spark.sql(
        """
          |insert into test_lh(siteid,citycode,username, pv) select siteid,citycode+1 as citycode,username,pv+1 as pv from table2
          |""".stripMargin)
      Thread.sleep(2000)
      val actual = ContainerUtils.executeSQLStatement(
        getKesQueryConnection,
        LOG,
        String.format("select * from %s.%s", DATABASE, kesPartialTable),
        5)
      val expected = util.Arrays.asList("0,1,user1,4,0");
      checkResultInAnyOrder("jsonDataWriteWithPartialUpdateSqlTest1", expected.toArray, actual.toArray)
    } finally {
      spark.stop()
    }
  }


  @Test
  def jsonDataWriteWithSchemaLess(): Unit = {
    initializeTable(kesSchemaLessTable, DataModel.UNIQUE)
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    try {
      val df = spark.createDataFrame(Seq(
        (0, 0, 100),
        (1, 0, 100),
        (3, 0, 200)
      )).toDF("siteid", "citycode", "pv")
      df.write
        .format("kes")
        .option("kes.fenodes", getFenodes)
        .option("kes.table.identifier", DATABASE + "." + kesSchemaLessTable)
        .option("kes.user", getKesUsername)
        .option("kes.password", getKesPassword)
        .option("kes.sink.properties.format", "json")
        .option("kes.sink.properties.compress_type", "")
        .option("kes.write.schemaless", "true")
        .option("sink.batch.size", 2)
        .option("sink.max-retries", 2)
        .mode(SaveMode.Append)
        .save()
      spark.stop()
      Thread.sleep(2000)
      val actual = ContainerUtils.executeSQLStatement(
        getKesQueryConnection,
        LOG,
        String.format("select * from %s.%s", DATABASE, kesSchemaLessTable),
        4)
      val expected = util.Arrays.asList("0,0,,100", "1,0,,100", "3,0,,200");
      checkResultInAnyOrder("jsonDataWriteWithSchemaLess", expected.toArray, actual.toArray)
    } finally {
      spark.stop()
    }
  }

  private def initializeTable(table: String, dataModel: DataModel): Unit = {
    val morProps = if (!(DataModel.UNIQUE_MOR == dataModel)) "" else ",\"enable_unique_key_merge_on_write\" = \"false\""
    val model = if (dataModel == DataModel.UNIQUE_MOR) DataModel.UNIQUE.toString else dataModel.toString
    ContainerUtils.executeSQLStatement(
      getKesQueryConnection,
      LOG,
      String.format("CREATE DATABASE IF NOT EXISTS %s", DATABASE),
      String.format("DROP TABLE IF EXISTS %s.%s", DATABASE, table),
      String.format("CREATE TABLE %s.%s ( \n"
        + " siteid INT DEFAULT '10',"
        + " citycode SMALLINT, "
        + " username VARCHAR(32) DEFAULT '',"
        + " pv BIGINT DEFAULT '0' "
        + " )"
        + " %s KEY(siteid, citycode, username) "
        + " DISTRIBUTED BY HASH(`siteid`) BUCKETS 1\n"
        + "PROPERTIES ("
        + "\"replication_num\" = \"1\"\n" + morProps + ")", DATABASE, table, model))
  }

  private def initializePartialTable(table: String, dataModel: DataModel): Unit = {
    val morProps = if (!(DataModel.UNIQUE_MOR == dataModel)) "" else ",\"enable_unique_key_merge_on_write\" = \"false\""
    val model = if (dataModel == DataModel.UNIQUE_MOR) DataModel.UNIQUE.toString else dataModel.toString
    ContainerUtils.executeSQLStatement(
      getKesQueryConnection,
      LOG,
      String.format("CREATE DATABASE IF NOT EXISTS %s", DATABASE),
      String.format("DROP TABLE IF EXISTS %s.%s", DATABASE, table),
      String.format("CREATE TABLE %s.%s ( \n"
        + " siteid INT DEFAULT '10',"
        + " citycode SMALLINT, "
        + " username VARCHAR(32) DEFAULT '',"
        + " pv BIGINT DEFAULT '0', "
        + " p_value BIGINT DEFAULT '0' "
        + " )"
        + " %s KEY(siteid, citycode, username) "
        + " DISTRIBUTED BY HASH(`siteid`) BUCKETS 1\n"
        + "PROPERTIES ("
        + "\"replication_num\" = \"1\"\n" + morProps + ")", DATABASE, table, model))
  }

  private def checkResultInAnyOrder(testName: String, expected: Array[AnyRef], actual: Array[AnyRef]): Unit = {
    LOG.info("Checking KesAnySchemaITCase result. testName={}, actual={}, expected={}", testName, actual, expected)
    assertEqualsInAnyOrder(expected.toList.asJava, actual.toList.asJava)
  }
}
