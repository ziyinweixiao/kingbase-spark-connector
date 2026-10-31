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

import com.kingbase.kes.spark.container.AbstractContainerTestBase.{assertEqualsInAnyOrder, getKesQueryConnection}
import com.kingbase.kes.spark.container.{AbstractContainerTestBase, ContainerUtils}
import com.kingbase.kes.spark.rest.models.DataModel
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.junit.Test
import org.slf4j.LoggerFactory

import java.util
import scala.collection.JavaConverters._

/**
 * it case for kes writer.
 */
class KesWriterITCase extends AbstractContainerTestBase {

  private val LOG = LoggerFactory.getLogger(classOf[KesWriterITCase])

  val DATABASE: String = "test_kes_write"
  val TABLE_CSV: String = "tbl_csv"
  val TABLE_CSV_BENODES: String = "tbl_csv_benodes"
  val TABLE_CSV_HIDE_SEP: String = "tbl_csv_hide_sep"
  val TABLE_GROUP_COMMIT: String = "tbl_group_commit"
  val TABLE_JSON: String = "tbl_json"
  val TABLE_JSON_EMPTY_PARTITION: String = "tbl_json_empty_partition"
  val TABLE_JSON_TBL: String = "tbl_json_tbl"
  val TABLE_JSON_TBL_OVERWRITE: String = "tbl_json_tbl_overwrite"
  val TABLE_JSON_TBL_ARROW: String = "tbl_json_tbl_arrow"
  val TABLE_BITMAP_TBL: String = "tbl_write_tbl_bitmap"
  val TABLE_UNICODE_COL: String = "tbl_unicode_col"
  val TABLE_CSV_NO_COMPRESS: String = "tbl_csv_no_compress"
  val TABLE_CSV_GZ_COMPRESS: String = "tbl_csv_gz_compress"

  @Test
  @throws[Exception]
  def testSinkCsvFormat(): Unit = {
    initializeTable(TABLE_CSV, DataModel.DUPLICATE)
    val session = SparkSession.builder().master("local[1]").getOrCreate()
    try {
      val df = session.createDataFrame(Seq(
        ("kes_csv", 1),
        ("spark_csv", 2)
      )).toDF("name", "age")
      df.write
        .format("kes")
        .option("kes.fenodes", getFenodes)
        .option("kes.sink.auto-redirect", false)
        .option("kes.table.identifier", DATABASE + "." + TABLE_CSV)
        .option("kes.user", getKesUsername)
        .option("kes.password", getKesPassword)
        .option("sink.properties.column_separator", ",")
        .option("sink.properties.line_delimiter", "\n")
        .option("sink.properties.format", "csv")
        .option("kes.sink.batch.interval.ms", "5000")
        .option("kes.sink.batch.size", "1")
        .mode(SaveMode.Append)
        .save()

      Thread.sleep(15000)
      val actual = ContainerUtils.executeSQLStatement(
        getKesQueryConnection,
        LOG,
        String.format("select * from %s.%s", DATABASE, TABLE_CSV),
        2)
      val expected = util.Arrays.asList("kes_csv,1", "spark_csv,2")
      checkResultInAnyOrder("testSinkCsvFormat", expected.toArray(), actual.toArray)
    } finally {
      session.stop()
    }
  }

  @Test
  @throws[Exception]
  def testSinkBeNodes(): Unit = {
    initializeTable(TABLE_CSV_BENODES, DataModel.DUPLICATE)
    val session = SparkSession.builder().master("local[1]").getOrCreate()
    try {
      val df = session.createDataFrame(Seq(
        ("kes_csv", 1),
        ("spark_csv", 2)
      )).toDF("name", "age")
      df.write
        .format("kes")
        .option("kes.fenodes", getFenodes)
        .option("kes.benodes", getBenodes)
        .option("kes.sink.auto-redirect", false)
        .option("kes.table.identifier", DATABASE + "." + TABLE_CSV_BENODES)
        .option("kes.user", getKesUsername)
        .option("kes.password", getKesPassword)
        .option("sink.properties.column_separator", ",")
        .option("sink.properties.line_delimiter", "\n")
        .option("sink.properties.format", "csv")
        .option("kes.sink.batch.interval.ms", "5000")
        .option("kes.sink.batch.size", "1")
        .mode(SaveMode.Append)
        .save()

      Thread.sleep(15000)
      val actual = ContainerUtils.executeSQLStatement(
        getKesQueryConnection,
        LOG,
        String.format("select * from %s.%s", DATABASE, TABLE_CSV_BENODES),
        2)
      val expected = util.Arrays.asList("kes_csv,1", "spark_csv,2")
      checkResultInAnyOrder("testSinkCsvFormat", expected.toArray(), actual.toArray)
    } finally {
      session.stop()
    }
  }

  @Test
  @throws[Exception]
  def testSinkCsvFormatHideSep(): Unit = {
    initializeTable(TABLE_CSV_HIDE_SEP, DataModel.AGGREGATE)
    val session = SparkSession.builder().master("local[*]").getOrCreate()
    try {
      val df = session.createDataFrame(Seq(
        ("kes_csv", 1),
        ("spark_csv", 2)
      )).toDF("name", "age")
      df.write
        .format("kes")
        .option("kes.fenodes", getFenodes + "," + getFenodes)
        .option("kes.table.identifier", DATABASE + "." + TABLE_CSV_HIDE_SEP)
        .option("kes.user", getKesUsername)
        .option("kes.password", getKesPassword)
        .option("sink.properties.column_separator", "\\x01")
        .option("sink.properties.line_delimiter", "\\x02")
        .option("sink.properties.format", "csv")
        .mode(SaveMode.Append)
        .save()

      Thread.sleep(10000)
      val actual = ContainerUtils.executeSQLStatement(
        getKesQueryConnection,
        LOG,
        String.format("select * from %s.%s", DATABASE, TABLE_CSV_HIDE_SEP),
        2)
      val expected = util.Arrays.asList("kes_csv,1", "spark_csv,2")
      checkResultInAnyOrder("testSinkCsvFormatHideSep", expected.toArray(), actual.toArray)
    } finally {
      session.stop()
    }
  }

  @Test
  @throws[Exception]
  def testSinkGroupCommit(): Unit = {
    initializeTable(TABLE_GROUP_COMMIT, DataModel.DUPLICATE)
    val session = SparkSession.builder().master("local[*]").getOrCreate()
    try {
      val df = session.createDataFrame(Seq(
        ("kes_csv", 1),
        ("spark_csv", 2)
      )).toDF("name", "age")
      df.coalesce(1).write
        .format("kes")
        .option("kes.fenodes", getFenodes)
        .option("kes.table.identifier", DATABASE + "." + TABLE_GROUP_COMMIT)
        .option("kes.user", getKesUsername)
        .option("kes.password", getKesPassword)
        .option("sink.properties.group_commit", "sync_mode")
        .mode(SaveMode.Append)
        .save()

      Thread.sleep(10000)
      val actual = ContainerUtils.executeSQLStatement(
        getKesQueryConnection,
        LOG,
        String.format("select * from %s.%s", DATABASE, TABLE_GROUP_COMMIT),
        2)
      val expected = util.Arrays.asList("kes_csv,1", "spark_csv,2")
      checkResultInAnyOrder("testSinkGroupCommit", expected.toArray(), actual.toArray)
    } finally {
      session.stop()
    }
  }

  @Test
  @throws[Exception]
  def testSinkEmptyPartition(): Unit = {
    initializeTable(TABLE_JSON_EMPTY_PARTITION, DataModel.AGGREGATE)
    val session = SparkSession.builder().master("local[2]").getOrCreate()
    try {
      val df = session.createDataFrame(Seq(
        ("kes_json", 1)
      )).toDF("name", "age")
      df.repartition(2).write
        .format("kes")
        .option("kes.fenodes", getFenodes)
        .option("kes.table.identifier", DATABASE + "." + TABLE_JSON_EMPTY_PARTITION)
        .option("kes.user", getKesUsername)
        .option("kes.password", getKesPassword)
        .option("sink.properties.read_json_by_line", "true")
        .option("sink.properties.format", "json")
        .option("sink.properties.compress_type", "")
        .option("kes.sink.auto-redirect", "false")
        .option("kes.sink.enable-2pc", "true")
        .mode(SaveMode.Append)
        .save()

      Thread.sleep(10000)
      val actual = ContainerUtils.executeSQLStatement(
        getKesQueryConnection,
        LOG,
        String.format("select * from %s.%s", DATABASE, TABLE_JSON_EMPTY_PARTITION),
        2)
      val expected = util.Arrays.asList("kes_json,1");
      checkResultInAnyOrder("testSinkEmptyPartition", expected.toArray, actual.toArray)
    } finally {
      session.stop()
    }
  }

  @Test
  @throws[Exception]
  def testSinkArrowFormat(): Unit = {
    initializeTable(TABLE_JSON_TBL_ARROW, DataModel.DUPLICATE)
    val session = SparkSession.builder().master("local[*]").getOrCreate()
    try {
      val df = session.createDataFrame(Seq(
        ("kes_json", 1),
        ("spark_json", 2)
      )).toDF("name", "age")
      df.write
        .format("kes")
        .option("kes.fenodes", getFenodes)
        .option("kes.table.identifier", DATABASE + "." + TABLE_JSON_TBL_ARROW)
        .option("kes.user", getKesUsername)
        .option("kes.password", getKesPassword)
        .option("sink.properties.format", "arrow")
        .option("kes.sink.batch.size", "1")
        .option("kes.sink.enable-2pc", "true")
        .mode(SaveMode.Append)
        .save()

      Thread.sleep(10000)
      val actual = ContainerUtils.executeSQLStatement(
        getKesQueryConnection,
        LOG,
        String.format("select * from %s.%s", DATABASE, TABLE_JSON_TBL_ARROW),
        2)
      val expected = util.Arrays.asList("kes_json,1", "spark_json,2");
      checkResultInAnyOrder("testSinkArrowFormat", expected.toArray, actual.toArray)
    } finally {
      session.stop()
    }
  }

  @Test
  @throws[Exception]
  def testSinkJsonFormat(): Unit = {
    initializeTable(TABLE_JSON, DataModel.UNIQUE)
    val session = SparkSession.builder().master("local[*]").getOrCreate()
    try {
      val df = session.createDataFrame(Seq(
        ("kes_json", 1),
        ("spark_json", 2)
      )).toDF("name", "age")
      df.write
        .format("kes")
        .option("kes.fenodes", getFenodes)
        .option("kes.table.identifier", DATABASE + "." + TABLE_JSON)
        .option("kes.user", getKesUsername)
        .option("kes.password", getKesPassword)
        .option("sink.properties.read_json_by_line", "true")
        .option("sink.properties.format", "json")
        .option("sink.properties.compress_type", "")
        .option("kes.sink.auto-redirect", "false")
        .mode(SaveMode.Append)
        .save()

      Thread.sleep(10000)
      val actual = ContainerUtils.executeSQLStatement(
        getKesQueryConnection,
        LOG,
        String.format("select * from %s.%s", DATABASE, TABLE_JSON),
        2)
      val expected = util.Arrays.asList("kes_json,1", "spark_json,2");
      checkResultInAnyOrder("testSinkJsonFormat", expected.toArray, actual.toArray)
    } finally {
      session.stop()
    }
  }

  @Test
  @throws[Exception]
  def testSQLSinkFormat(): Unit = {
    initializeTable(TABLE_JSON_TBL, DataModel.UNIQUE_MOR)
    val session = SparkSession.builder().master("local[*]").getOrCreate()
    try {
      val df = session.createDataFrame(Seq(
        ("kes_tbl", 1),
        ("spark_tbl", 2)
      )).toDF("name", "age")
      df.createTempView("mock_source")
      session.sql(
        s"""
           |CREATE TEMPORARY VIEW test_sink
           |USING kes
           |OPTIONS(
           | "table.identifier"="${DATABASE + "." + TABLE_JSON_TBL}",
           | "fenodes"="${getFenodes}",
           | "user"="${getKesUsername}",
           | "password"="${getKesPassword}"
           |)
           |""".stripMargin)
      session.sql(
        """
          |insert into test_sink select  name,age from mock_source
          |""".stripMargin)

      Thread.sleep(10000)
      val actual = ContainerUtils.executeSQLStatement(
        getKesQueryConnection,
        LOG,
        String.format("select * from %s.%s", DATABASE, TABLE_JSON_TBL),
        2)
      val expected = util.Arrays.asList("kes_tbl,1", "spark_tbl,2");
      checkResultInAnyOrder("testSQLSinkFormat", expected.toArray, actual.toArray)
    } finally {
      session.stop()
    }
  }

  @Test
  @throws[Exception]
  def testSQLSinkOverwrite(): Unit = {
    initializeTable(TABLE_JSON_TBL_OVERWRITE, DataModel.DUPLICATE)
    // init history data
    ContainerUtils.executeSQLStatement(
      getKesQueryConnection,
      LOG,
      String.format("insert into %s.%s  values ('history-kes',1118)", DATABASE, TABLE_JSON_TBL_OVERWRITE),
      String.format("insert into %s.%s  values ('history-spark',1110)", DATABASE, TABLE_JSON_TBL_OVERWRITE))

    val session = SparkSession.builder().master("local[*]").getOrCreate()
    try {
      val df = session.createDataFrame(Seq(
        ("kes_tbl", 1),
        ("spark_tbl", 2)
      )).toDF("name", "age")
      df.createTempView("mock_source")
      session.sql(
        s"""
           |CREATE TEMPORARY VIEW test_sink
           |USING kes
           |OPTIONS(
           | "table.identifier"="${DATABASE + "." + TABLE_JSON_TBL_OVERWRITE}",
           | "fenodes"="${getFenodes}",
           | "user"="${getKesUsername}",
           | "password"="${getKesPassword}",
           | "kes.query.port"="${getQueryPort}",
           | "kes.sink.label.prefix"="kes-label-customer",
           | "kes.sink.enable-2pc"="true"
           |)
           |""".stripMargin)
      session.sql(
        """
          |insert overwrite table test_sink select  name,age from mock_source
          |""".stripMargin)

      Thread.sleep(10000)
      val actual = ContainerUtils.executeSQLStatement(
        getKesQueryConnection,
        LOG,
        String.format("select * from %s.%s", DATABASE, TABLE_JSON_TBL_OVERWRITE),
        2)
      val expected = util.Arrays.asList("kes_tbl,1", "spark_tbl,2");
      checkResultInAnyOrder("testSQLSinkOverwrite", expected.toArray, actual.toArray)
    } finally {
      session.stop()
    }
  }

  @Test
  def testWriteBitmap(): Unit = {
    val targetInitSql: Array[String] = ContainerUtils.parseFileContentSQL("container/ddl/write_bitmap.sql")
    ContainerUtils.executeSQLStatement(getKesQueryConnection(DATABASE), LOG, targetInitSql: _*)

    val session = SparkSession.builder().master("local[*]").getOrCreate()
    try {
      val df = session.createDataFrame(Seq(
        (20200621, 1, "243"),
        (20200622, 2, "1"),
        (20200623, 3, "287667876573")
      )).toDF("datekey", "hour", "device_id")
      df.createTempView("mock_source")
      session.sql(
        s"""
           |CREATE TEMPORARY VIEW test_sink
           |USING kes
           |OPTIONS(
           | "table.identifier"="${DATABASE + "." + TABLE_BITMAP_TBL}",
           | "fenodes"="${getFenodes}",
           | "user"="${getKesUsername}",
           | "password"="${getKesPassword}",
           | "kes.write.fields"="datekey,hour,device_id,device_id=to_bitmap(device_id)"
           |)
           |""".stripMargin)
      session.sql(
        """
          |insert into test_sink select datekey,hour,device_id from mock_source
          |""".stripMargin)

      Thread.sleep(10000)
      val actual = ContainerUtils.executeSQLStatement(
        getKesQueryConnection,
        LOG,
        String.format("select datekey,hour,bitmap_to_string(device_id) from %s.%s", DATABASE, TABLE_BITMAP_TBL),
        3)
      val expected = util.Arrays.asList("20200621,1,243", "20200622,2,1", "20200623,3,287667876573");
      checkResultInAnyOrder("testWriteBitmap", expected.toArray, actual.toArray)
    } finally {
      session.stop()
    }
  }


  @Test
  @throws[Exception]
  def testSinkCsvGzCompression(): Unit = {
    initializeTable(TABLE_CSV_GZ_COMPRESS, DataModel.DUPLICATE)
    val session = SparkSession.builder().master("local[1]").getOrCreate()
    try {
      val df = session.createDataFrame(Seq(
        ("kes_gz", 1),
        ("spark_gz", 2)
      )).toDF("name", "age")
      df.write
        .format("kes")
        .option("kes.fenodes", getFenodes)
        .option("kes.table.identifier", DATABASE + "." + TABLE_CSV_GZ_COMPRESS)
        .option("kes.user", getKesUsername)
        .option("kes.password", getKesPassword)
        .option("sink.properties.column_separator", ",")
        .option("sink.properties.line_delimiter", "\n")
        .option("sink.properties.format", "csv")
        .option("sink.properties.compress_type", "gz")
        .mode(SaveMode.Append)
        .save()

      Thread.sleep(15000)
      val actual = ContainerUtils.executeSQLStatement(
        getKesQueryConnection,
        LOG,
        String.format("select * from %s.%s", DATABASE, TABLE_CSV_GZ_COMPRESS),
        2)
      val expected = util.Arrays.asList("kes_gz,1", "spark_gz,2")
      checkResultInAnyOrder("testSinkCsvGzCompression", expected.toArray(), actual.toArray)
    } finally {
      session.stop()
    }
  }

  @Test
  @throws[Exception]
  def testSinkCsvNoCompression(): Unit = {
    initializeTable(TABLE_CSV_NO_COMPRESS, DataModel.DUPLICATE)
    val session = SparkSession.builder().master("local[1]").getOrCreate()
    try {
      val df = session.createDataFrame(Seq(
        ("kes_no_compress", 1),
        ("spark_no_compress", 2)
      )).toDF("name", "age")
      df.write
        .format("kes")
        .option("kes.fenodes", getFenodes)
        .option("kes.table.identifier", DATABASE + "." + TABLE_CSV_NO_COMPRESS)
        .option("kes.user", getKesUsername)
        .option("kes.password", getKesPassword)
        .option("sink.properties.column_separator", ",")
        .option("sink.properties.line_delimiter", "\n")
        .option("sink.properties.format", "csv")
        .option("sink.properties.compress_type", "")
        .mode(SaveMode.Append)
        .save()

      Thread.sleep(15000)
      val actual = ContainerUtils.executeSQLStatement(
        getKesQueryConnection,
        LOG,
        String.format("select * from %s.%s", DATABASE, TABLE_CSV_NO_COMPRESS),
        2)
      val expected = util.Arrays.asList("kes_no_compress,1", "spark_no_compress,2")
      checkResultInAnyOrder("testSinkCsvNoCompression", expected.toArray(), actual.toArray)
    } finally {
      session.stop()
    }
  }

  private def initializeTable(table: String, dataModel: DataModel): Unit = {
    val max = if (DataModel.AGGREGATE == dataModel) "MAX" else ""
    val morProps = if (!(DataModel.UNIQUE_MOR == dataModel)) "" else ",\"enable_unique_key_merge_on_write\" = \"false\""
    val model = if (dataModel == DataModel.UNIQUE_MOR) DataModel.UNIQUE.toString else dataModel.toString
    ContainerUtils.executeSQLStatement(
      getKesQueryConnection,
      LOG,
      String.format("CREATE DATABASE IF NOT EXISTS %s", DATABASE),
      String.format("DROP TABLE IF EXISTS %s.%s", DATABASE, table),
      String.format("CREATE TABLE %s.%s ( \n"
        + "`name` varchar(256),\n"
        + "`age` int %s\n"
        + ") "
        + " %s KEY(`name`) "
        + " DISTRIBUTED BY HASH(`name`) BUCKETS 1\n"
        + "PROPERTIES ("
        + "\"replication_num\" = \"1\"\n" + morProps + ")", DATABASE, table, max, model))
  }

  private def checkResultInAnyOrder(testName: String, expected: Array[AnyRef], actual: Array[AnyRef]): Unit = {
    LOG.info("Checking KesWriterITCase result. testName={}, actual={}, expected={}", testName, actual, expected)
    assertEqualsInAnyOrder(expected.toList.asJava, actual.toList.asJava)
  }

  @Test
  def testWriteUnicodeColumn(): Unit = {
    val createDb = String.format("CREATE DATABASE IF NOT EXISTS %s", DATABASE)
    val targetInitSql: Array[String] = ContainerUtils.parseFileContentSQL("container/ddl/write_unicode_col.sql")
    ContainerUtils.executeSQLStatement(getKesQueryConnection, LOG, Array(createDb): _*)
    val connection = getKesQueryConnection(DATABASE)
    ContainerUtils.executeSQLStatement(connection, LOG, targetInitSql: _*)

    val session = SparkSession.builder().master("local[*]").getOrCreate()
    try {
      val df = session.createDataFrame(Seq(
        (1, "243"),
        (2, "1"),
        (3, "287667876573")
      )).toDF("序号", "内容")
      df.createTempView("mock_source")
      session.sql(
        s"""
           |CREATE TEMPORARY VIEW test_sink
           |USING kes
           |OPTIONS(
           | "table.identifier"="${DATABASE + "." + TABLE_UNICODE_COL}",
           | "fenodes"="${getFenodes}",
           | "user"="${getKesUsername}",
           | "password"="${getKesPassword}",
           | "kes.sink.http-utf8-charset"="true"
           |)
           |""".stripMargin)
      session.sql(
        """
          |insert into test_sink select `序号`,`内容` from mock_source
          |""".stripMargin)

      Thread.sleep(10000)
      val actual = ContainerUtils.executeSQLStatement(
        connection,
        LOG,
        String.format("select `序号`,`内容` from %s.%s", DATABASE, TABLE_UNICODE_COL),
        2)
      val expected = util.Arrays.asList("1,243", "2,1", "3,287667876573");
      checkResultInAnyOrder("testWriteUnicodeColumn", expected.toArray, actual.toArray)
    } finally {
      session.stop()
    }
  }

}
