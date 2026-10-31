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
import com.kingbase.kes.spark.config.KesOptions
import com.kingbase.kes.spark.exception.KesException
import org.apache.spark.sql.jdbc.JdbcDialect
import org.apache.spark.sql.sources._
import org.slf4j.Logger

import java.sql.{Date, Timestamp}
import java.time.{Duration, LocalDate}
import java.util.concurrent.locks.LockSupport
import scala.annotation.tailrec
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

private[spark] object Utils {
  /**
   * quote column name
   *
   * @param colName column name
   * @return quoted column name
   */
  def quote(colName: String): String = s"`$colName`"

  /**
   * compile a filter to Kes FE filter format.
   *
   * @param filter             filter to be compile
   * @param dialect            jdbc dialect to translate value to sql format
   * @param inValueLengthLimit max length of in value array
   * @return if Kes FE can handle this filter, return None if Kes FE can not handled it.
   */
  def compileFilter(filter: Filter, dialect: JdbcDialect, inValueLengthLimit: Int): Option[String] = {
    Option(filter match {
      case EqualTo(attribute, value) => s"${quote(attribute)} = ${compileValue(value)}"
      case Not(EqualTo(attribute, value)) => s"${quote(attribute)} != ${compileValue(value)}"
      case GreaterThan(attribute, value) => s"${quote(attribute)} > ${compileValue(value)}"
      case GreaterThanOrEqual(attribute, value) => s"${quote(attribute)} >= ${compileValue(value)}"
      case LessThan(attribute, value) => s"${quote(attribute)} < ${compileValue(value)}"
      case LessThanOrEqual(attribute, value) => s"${quote(attribute)} <= ${compileValue(value)}"
      case In(attribute, values) =>
        if (values.isEmpty || values.length >= inValueLengthLimit) {
          null
        } else {
          s"${quote(attribute)} in (${compileValue(values)})"
        }
      case Not(In(attribute, values)) =>
        if (values.isEmpty || values.length >= inValueLengthLimit) {
          null
        } else {
          s"${quote(attribute)} not in (${compileValue(values)})"
        }
      case IsNull(attribute) => s"${quote(attribute)} is null"
      case IsNotNull(attribute) => s"${quote(attribute)} is not null"
      case And(left, right) =>
        val and = Seq(left, right).flatMap(compileFilter(_, dialect, inValueLengthLimit))
        if (and.size == 2) {
          and.map(p => s"($p)").mkString(" and ")
        } else {
          null
        }
      case Or(left, right) =>
        val or = Seq(left, right).flatMap(compileFilter(_, dialect, inValueLengthLimit))
        if (or.size == 2) {
          or.map(p => s"($p)").mkString(" or ")
        } else {
          null
        }
      case StringContains(attribute, value) => s"${quote(attribute)} like '%$value%'"
      case Not(StringContains(attribute, value)) => s"${quote(attribute)} not like '%$value%'"
      case StringEndsWith(attribute, value) => s"${quote(attribute)} like '%$value'"
      case Not(StringEndsWith(attribute, value)) => s"${quote(attribute)} not like '%$value'"
      case StringStartsWith(attribute, value) => s"${quote(attribute)} like '$value%'"
      case Not(StringStartsWith(attribute, value)) => s"${quote(attribute)} not like '$value%'"
      case _ => null
    })
  }

  /**
   * Escape special characters in SQL string literals.
   *
   * @param value The string to be escaped.
   * @return Escaped string.
   */
  private def escapeSql(value: String): String =
    if (value == null) null else StringUtils.replace(value, "'", "''")

  /**
   * Converts value to SQL expression.
   *
   * @param value The value to be converted.
   * @return Converted value.
   */
  private def compileValue(value: Any): Any = value match {
    case stringValue: String => s"'${escapeSql(stringValue)}'"
    case timestampValue: Timestamp => "'" + timestampValue + "'"
    case dateValue: Date => "'" + dateValue + "'"
    case dateValue: LocalDate => "'" + dateValue + "'"
    case arrayValue: Array[Any] => arrayValue.map(compileValue).mkString(", ")
    case _ => value
  }

  /**
   * check parameters validation and process it.
   *
   * @param parameters parameters from rdd and spark conf
   * @param logger     slf4j logger
   * @return processed parameters
   */
  def params(parameters: Map[String, String], logger: Logger) = {
    // '.' seems to be problematic when specifying the options
    val dottedParams = parameters.map { case (k, v) =>
      if (k.startsWith("sink.properties.") || k.startsWith("kes.sink.properties.")) {
        (k, v)
      } else {
        (k.replace('_', '.'), v)
      }
    }

    val preferredTableIdentifier = dottedParams.get(KesOptions.KES_TABLE_IDENTIFIER.getName)
    logger.debug(s"preferred Table Identifier is '$preferredTableIdentifier'.")

    // Convert simple parameters into internal properties, and prefix other parameters
    // Convert password parameters from "password" into internal password properties
    // reuse credentials mask method in spark ExternalCatalogUtils￿#maskCredentials
    val processedParams = dottedParams.map {
      case ("kes.password", _) =>
        logger.error(s"${KesOptions.KES_PASSWORD.getName} cannot use in Kes Datasource.")
        throw new KesException(s"${KesOptions.KES_PASSWORD.getName} cannot use in Kes Datasource," +
          s" use 'password' option to set password.")
      case ("kes.user", _) =>
        logger.error(s"${KesOptions.KES_USER.getName} cannot use in Kes Datasource.")
        throw new KesException(s"${KesOptions.KES_USER.getName} cannot use in Kes Datasource," +
          s" use 'user' option to set user.")
      case (k, v) =>
        if (k.startsWith("kes.")) (k, v)
        else ("kes." + k, v)
    }.map {
      case (KesOptions.KES_REQUEST_AUTH_PASSWORD, _) =>
        logger.error(s"${KesOptions.KES_REQUEST_AUTH_PASSWORD} cannot use in Kes Datasource.")
        throw new KesException(s"${KesOptions.KES_REQUEST_AUTH_PASSWORD} cannot use in" +
          s" Kes Datasource, use 'password' option to set password.")
      case (KesOptions.KES_REQUEST_AUTH_USER, _) =>
        logger.error(s"${KesOptions.KES_REQUEST_AUTH_USER} cannot use in Kes Datasource.")
        throw new KesException(s"${KesOptions.KES_REQUEST_AUTH_USER} cannot use in" +
          s" Kes Datasource, use 'user' option to set user.")
      case ("kes.password", v) =>
        (KesOptions.KES_REQUEST_AUTH_PASSWORD, v)
      case ("kes.user", v) =>
        (KesOptions.KES_REQUEST_AUTH_USER, v)
      case (k, v) => (k, v)
    }

    // Set the preferred resource if it was specified originally
    val finalParams = preferredTableIdentifier match {
      case Some(tableIdentifier) => processedParams + (KesOptions.KES_TABLE_IDENTIFIER.getName -> tableIdentifier)
      case None => processedParams
    }

    // validate path is available
    finalParams.getOrElse(KesOptions.KES_TABLE_IDENTIFIER.getName,
      throw new KesException("table identifier must be specified for kes table identifier."))

    finalParams
  }

  @tailrec
  def retry[R, T <: Throwable : ClassTag](retryTimes: Int, interval: Duration, logger: Logger)
                                         (f: => R)(h: => Unit): Try[R] = {
    assert(retryTimes >= 0)
    val result = Try(f)
    result match {
      case Success(result) =>
        Success(result)
      case Failure(exception: T) if retryTimes > 0 =>
        logger.warn(s"Execution failed caused by: ", exception)
        logger.warn(s"$retryTimes times retry remaining, the next attempt will be in ${interval.toMillis} ms")
        LockSupport.parkNanos(interval.toNanos)
        h
        retry(retryTimes - 1, interval, logger)(f)(h)
      case Failure(exception) => Failure(exception)
    }
  }

}
