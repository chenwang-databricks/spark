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

package org.apache.spark.sql.execution

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.connector.catalog.{Identifier, InMemoryTableCatalog, Table, TableCatalog, TableInfo, TableSummary}
import org.apache.spark.sql.metricview.serde.{AssetSource, Column, DimensionExpression, MeasureExpression, MetricView, MetricViewFactory, SQLSource}
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Tests that exercise [[org.apache.spark.sql.execution.command.CreateMetricViewCommand]] on a
 * non-session V2 catalog. Ensures the TableInfo produced for the V2 catalog carries the
 * metric view type, view definition, view dependencies, and the `metric_view.*` and
 * `view.*` table properties expected by downstream catalogs.
 */
class MetricViewV2CatalogSuite extends QueryTest with SharedSparkSession {

  import testImplicits._

  private val testCatalogName = "testcat"
  private val testNamespace = "ns"
  private val sourceTableName = "events"
  private val fullSourceTableName =
    s"$testCatalogName.$testNamespace.$sourceTableName"
  private val metricViewName = "mv"
  private val fullMetricViewName =
    s"$testCatalogName.$testNamespace.$metricViewName"

  private val metricViewColumns = Seq(
    Column("region", DimensionExpression("region"), 0),
    Column("count_sum", MeasureExpression("sum(count)"), 1))

  private val testTableData = Seq(
    ("region_1", 1, 5.0),
    ("region_2", 2, 10.0))

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(
      s"spark.sql.catalog.$testCatalogName",
      classOf[MetricViewRecordingCatalog].getName)
  }

  override protected def afterAll(): Unit = {
    spark.conf.unset(s"spark.sql.catalog.$testCatalogName")
    super.afterAll()
  }

  private def withTestCatalogTables(body: => Unit): Unit = {
    MetricViewRecordingCatalog.reset()
    testTableData.toDF("region", "count", "price")
      .createOrReplaceTempView("metric_view_v2_source")
    try {
      sql(
        s"""CREATE TABLE $fullSourceTableName
           |USING foo AS SELECT * FROM metric_view_v2_source""".stripMargin)
      body
    } finally {
      sql(s"DROP TABLE IF EXISTS $fullMetricViewName")
      sql(s"DROP TABLE IF EXISTS $fullSourceTableName")
      spark.catalog.dropTempView("metric_view_v2_source")
      MetricViewRecordingCatalog.reset()
    }
  }

  private def createMetricView(
      name: String,
      metricView: MetricView,
      comment: Option[String] = None): String = {
    val yaml = MetricViewFactory.toYAML(metricView)
    val commentClause = comment.map(c => s"\nCOMMENT '$c'").getOrElse("")
    sql(
      s"""CREATE VIEW $name
         |WITH METRICS$commentClause
         |LANGUAGE YAML
         |AS
         |$$$$
         |$yaml
         |$$$$""".stripMargin)
    yaml
  }

  private def capturedTableInfo(): TableInfo = {
    val ident = Identifier.of(Array(testNamespace), metricViewName)
    val info = MetricViewRecordingCatalog.captured.get(ident)
    assert(info != null,
      s"Expected TableInfo for $ident to be captured by the V2 catalog")
    info
  }

  test("V2 catalog receives METRIC_VIEW table type and view definition") {
    withTestCatalogTables {
      val metricView = MetricView(
        "0.1",
        AssetSource(fullSourceTableName),
        where = None,
        select = metricViewColumns)
      val yaml = createMetricView(fullMetricViewName, metricView)

      val info = capturedTableInfo()
      assert(info.tableType() === TableSummary.METRIC_VIEW_TABLE_TYPE)
      assert(info.viewDefinition().contains(yaml.trim))
      val deps = info.viewDependencies()
      assert(deps != null)
      assert(deps.dependencies().length === 1)
      val tableDep = deps.dependencies()(0).asInstanceOf[
        org.apache.spark.sql.connector.catalog.TableDependency]
      assert(tableDep.tableFullName() === fullSourceTableName)
    }
  }

  test("V2 catalog path populates metric_view.* and view.* properties") {
    withTestCatalogTables {
      val metricView = MetricView(
        "0.1",
        AssetSource(fullSourceTableName),
        where = Some("count > 0"),
        select = metricViewColumns)
      createMetricView(fullMetricViewName, metricView)

      val props = capturedTableInfo().properties()

      // metric_view.* descriptive properties (mirrors DBR SingleSourceMetricView).
      assert(props.get(MetricView.PROP_FROM_TYPE) === "ASSET")
      assert(props.get(MetricView.PROP_FROM_NAME) === fullSourceTableName)
      assert(props.get(MetricView.PROP_FROM_SQL) === null)
      assert(props.get(MetricView.PROP_WHERE) === "count > 0")

      // view.* properties produced by ViewHelper.generateViewProperties, needed
      // to re-parse the view text consistently when the metric view is loaded.
      import org.apache.spark.sql.catalyst.catalog.CatalogTable
      assert(props.containsKey(CatalogTable.VIEW_CATALOG_AND_NAMESPACE),
        s"Expected ${CatalogTable.VIEW_CATALOG_AND_NAMESPACE} in $props")
      val sqlConfigKeys = props.keySet().stream()
        .filter(_.startsWith(CatalogTable.VIEW_SQL_CONFIG_PREFIX))
        .count()
      assert(sqlConfigKeys > 0,
        s"Expected at least one ${CatalogTable.VIEW_SQL_CONFIG_PREFIX}* property in $props")
    }
  }

  test("V2 catalog path captures SQL source and comment") {
    withTestCatalogTables {
      val metricView = MetricView(
        "0.1",
        SQLSource(s"SELECT * FROM $fullSourceTableName"),
        where = None,
        select = metricViewColumns)
      createMetricView(fullMetricViewName, metricView, comment = Some("my mv"))

      val info = capturedTableInfo()
      assert(info.tableType() === TableSummary.METRIC_VIEW_TABLE_TYPE)

      val props = info.properties()
      assert(props.get(MetricView.PROP_FROM_TYPE) === "SQL")
      assert(props.get(MetricView.PROP_FROM_NAME) === null)
      assert(props.get(MetricView.PROP_FROM_SQL) ===
        s"SELECT * FROM $fullSourceTableName")
      assert(props.get(TableCatalog.PROP_COMMENT) === "my mv")

      // SQL source still produces the source-table dependency via the analyzed plan.
      val deps = info.viewDependencies()
      assert(deps != null && deps.dependencies().length === 1)
      val tableDep = deps.dependencies()(0).asInstanceOf[
        org.apache.spark.sql.connector.catalog.TableDependency]
      assert(tableDep.tableFullName() === fullSourceTableName)
    }
  }
}

/**
 * Minimal V2 catalog used by [[MetricViewV2CatalogSuite]] to capture the full
 * [[TableInfo]] passed to `createTable(Identifier, TableInfo)` so tests can
 * assert on `tableType`, `viewDefinition`, `viewDependencies`, and properties.
 */
class MetricViewRecordingCatalog extends InMemoryTableCatalog {
  override def createTable(ident: Identifier, tableInfo: TableInfo): Table = {
    MetricViewRecordingCatalog.captured.put(ident, tableInfo)
    super.createTable(ident, tableInfo)
  }
}

object MetricViewRecordingCatalog {
  val captured: ConcurrentHashMap[Identifier, TableInfo] =
    new ConcurrentHashMap[Identifier, TableInfo]()

  def reset(): Unit = captured.clear()
}
