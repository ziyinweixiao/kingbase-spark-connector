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
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.junit.Test
import org.slf4j.LoggerFactory

import java.util
import scala.collection.JavaConverters._

/**
 * it case for kes catalog.
 */
class KesCatalogITCase extends AbstractContainerTestBase {

  private val LOG = LoggerFactory.getLogger(classOf[KesCatalogITCase])
  private val DATABASE = "test_catalog"
  private val TBL_CATALOG = "tbl_catalog"

  @Test
  @throws[Exception]
  def testSparkCatalog(): Unit = {

    val conf = new SparkConf()
    conf.set("spark.sql.catalog.kes_catalog", "com.kingbase.kes.spark.catalog.KesTableCatalog")
    conf.set("spark.sql.catalog.kes_catalog.kes.fenodes", getFenodes)
    conf.set("spark.sql.catalog.kes_catalog.kes.query.port", getQueryPort.toString)
    conf.set("spark.sql.catalog.kes_catalog.kes.user", getKesUsername)
    conf.set("spark.sql.catalog.kes_catalog.kes.password", getKesPassword)
    val session = SparkSession.builder().config(conf).master("local[*]").getOrCreate()

    // session.sessionState.catalogManager.setCurrentCatalog("kes_catalog")
    // spark 2 no catalogManager property, used reflect
    try {
      val stateObj = session.sessionState
      val catalogManagerObj = stateObj.getClass.getMethod("catalogManager").invoke(stateObj)
      val setCurrentCatalogMethod = catalogManagerObj.getClass.getMethod("setCurrentCatalog", classOf[String])
      setCurrentCatalogMethod.invoke(catalogManagerObj, "kes_catalog")
    } catch {
      case e: Exception =>
        // if Spark 2，will throw NoSuchMethodException
        println("Catalog API not available, skipping catalog operations")
        e.printStackTrace()
        return
    }

    // show databases
    val showDatabaseActual = new util.ArrayList[String](session.sql("show databases").collect().map(_.getAs[String]("namespace")).toList.asJava)
    showDatabaseActual.add("information_schema")
    val showDatabaseExcept = ContainerUtils.executeSQLStatement(
      getKesQueryConnection,
      LOG,
      String.format("show databases"),
      1)
    checkResultInAnyOrder("testSparkCatalog", showDatabaseExcept.toArray, showDatabaseActual.toArray)

    ContainerUtils.executeSQLStatement(
      getKesQueryConnection,
      LOG,
      String.format("CREATE DATABASE IF NOT EXISTS %s", DATABASE))

    // mock data
    ContainerUtils.executeSQLStatement(
      getKesQueryConnection,
      LOG,
      String.format("CREATE DATABASE IF NOT EXISTS %s", DATABASE),
      String.format("DROP TABLE IF EXISTS %s.%s", DATABASE, TBL_CATALOG),
      String.format("CREATE TABLE %s.%s ( \n"
        + "`name` varchar(256),\n"
        + "`age` int\n"
        + ") "
        + " DUPLICATE KEY(`name`) "
        + " DISTRIBUTED BY HASH(`name`) BUCKETS 1\n"
        + "PROPERTIES ("
        + "\"replication_num\" = \"1\")", DATABASE, TBL_CATALOG),
      String.format("insert into %s.%s  values ('kes',18)", DATABASE, TBL_CATALOG),
      String.format("insert into %s.%s  values ('spark',10)", DATABASE, TBL_CATALOG)
    )

    // show tables
    session.sql("USE " + DATABASE);
    val showTablesActual = session.sql("show tables").collect().map(_.getAs[String]("tableName")).toList.asJava
    val showTablesExcept = ContainerUtils.executeSQLStatement(
      getKesQueryConnection(DATABASE),
      LOG,
      String.format("show tables"),
      1)
    checkResultInAnyOrder("testSparkCatalog", showTablesExcept.toArray, showTablesActual.toArray)

    val query = String.format("select * from %s.%s", DATABASE, TBL_CATALOG)
    // select tables
    val selectActual = session.sql(query).collect().map(i=> i.getAs[String]("name") + "," + i.getAs[Int]("age")).toList.asJava
    val selectExcept = ContainerUtils.executeSQLStatement(
      getKesQueryConnection(DATABASE),
      LOG,
      query,
      2)
    checkResultInAnyOrder("testSparkCatalog", selectExcept.toArray, selectActual.toArray)

    session.sql(String.format("desc %s",TBL_CATALOG)).show(true);
    // insert tables
    // todo: insert into values('') schema does not match
    session.sql(String.format("insert overwrite %s.%s select 'insert-data' as name, 99 as age", DATABASE, TBL_CATALOG))
    val selectNewExcept = ContainerUtils.executeSQLStatement(
      getKesQueryConnection(DATABASE),
      LOG,
      query,
      2)
    checkResultInAnyOrder("testSparkCatalog", selectNewExcept.toArray, util.Arrays.asList("insert-data,99").toArray)
  }


  private def checkResultInAnyOrder(testName: String, expected: Array[AnyRef], actual: Array[AnyRef]): Unit = {
    LOG.info("Checking KesCatalogITCase result. testName={}, actual={}, expected={}", testName, actual, expected)
    assertEqualsInAnyOrder(expected.toList.asJava, actual.toList.asJava)
  }

}
