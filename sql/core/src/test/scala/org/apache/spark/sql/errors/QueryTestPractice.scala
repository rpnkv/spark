/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.errors

// import org.apache.spark.{SparkArithmeticException, SparkConf}

import org.apache.spark.{SparkArithmeticException, SparkConf}
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.ExtendedAnalysisException
import org.apache.spark.sql.catalyst.plans.logical.OneRowRelation
import org.apache.spark.sql.functions.lit
// import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession


class QueryTestPractice extends QueryTest with SharedSparkSession {
  override def sparkConf: SparkConf = super.sparkConf.set(SQLConf.ANSI_ENABLED.key, "true")

  private val ansiConf = "\"" + SQLConf.ANSI_ENABLED.key + "\""

  test("test 1") {
    sql("select 1")
  }

  test("test 2") {
    intercept[ExtendedAnalysisException] {
      sql("select 2 from non_existing_table")
    }
  }

  test("test 5") {
    spark.range(20).count()
  }


  test("test 6") {
    checkError(
      exception = intercept[SparkArithmeticException] {
        // OneRowRelation().select(lit(5) / lit(0)).collect()
        sql("select 2142/0").collect()
      },
      parameters = Map("config" -> "\"spark.sql.ansi.enabled\""),
      sqlState = "22012",
      condition = "DIVIDE_BY_ZERO",
      context = ExpectedContext(
        fragment = "2142/0"
      )
    )
  }

  test("test NUMERIC_VALUE_OUT_OF_RANGE") {
    intercept[ArithmeticException] {
      OneRowRelation().select(lit(Int.MaxValue) + lit(1)).collect()
    }
  }

  //  test("test 8") {
  //    sql("select 5/0").collect()
  //  }
  //
  //  test("test 3") {
  //    checkError(
  //      exception = intercept[ExtendedAnalysisException](sql("select 2 from non_existing_table")),
  //      condition = "",
  //    )
  //  }
}
