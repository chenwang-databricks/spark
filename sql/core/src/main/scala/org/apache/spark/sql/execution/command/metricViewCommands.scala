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

package org.apache.spark.sql.execution.command

import org.apache.spark.SparkException
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.{QueryPlanningTracker, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.{ResolvedIdentifier, SchemaUnsupported}
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, Dependency, DependencyList, TableInfo, TableSummary}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.metricview.serde.{AssetSource, MetricViewFactory}
import org.apache.spark.sql.metricview.util.MetricViewPlanner
import org.apache.spark.sql.types.StructType

case class CreateMetricViewCommand(
    child: LogicalPlan,
    userSpecifiedColumns: Seq[(String, Option[String])],
    comment: Option[String],
    properties: Map[String, String],
    originalText: String,
    allowExisting: Boolean,
    replace: Boolean) extends UnaryRunnableCommand {

  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  override def run(sparkSession: SparkSession): Seq[Row] = {
    child match {
      case v: ResolvedIdentifier if !CatalogV2Util.isSessionCatalog(v.catalog) =>
        createMetricViewInV2Catalog(sparkSession, v)
      case v: ResolvedIdentifier =>
        createMetricViewInSessionCatalog(sparkSession, v)
      case _ => throw SparkException.internalError(
        s"Failed to resolve identifier for creating metric view")
    }
  }

  private def createMetricViewInSessionCatalog(
      sparkSession: SparkSession,
      resolved: ResolvedIdentifier): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    val name = resolved.identifier.asTableIdentifier
    val analyzed = MetricViewHelper.analyzeMetricViewText(sparkSession, name, originalText)

    if (userSpecifiedColumns.nonEmpty) {
      if (userSpecifiedColumns.length > analyzed.output.length) {
        throw QueryCompilationErrors.cannotCreateViewNotEnoughColumnsError(
          name, userSpecifiedColumns.map(_._1), analyzed)
      } else if (userSpecifiedColumns.length < analyzed.output.length) {
        throw QueryCompilationErrors.cannotCreateViewTooManyColumnsError(
          name, userSpecifiedColumns.map(_._1), analyzed)
      }
    }
    catalog.createTable(
      ViewHelper.prepareTable(
        sparkSession, name, Some(originalText), analyzed, userSpecifiedColumns,
        properties, SchemaUnsupported, comment,
        None, isMetricView = true),
      ignoreIfExists = allowExisting)
    Seq.empty
  }

  private def createMetricViewInV2Catalog(
      sparkSession: SparkSession,
      resolved: ResolvedIdentifier): Seq[Row] = {
    val tableCatalog = resolved.catalog.asTableCatalog
    val ident = resolved.identifier
    val name = ident.asTableIdentifier

    val analyzed = MetricViewHelper.analyzeMetricViewText(sparkSession, name, originalText)

    if (userSpecifiedColumns.nonEmpty) {
      if (userSpecifiedColumns.length > analyzed.output.length) {
        throw QueryCompilationErrors.cannotCreateViewNotEnoughColumnsError(
          name, userSpecifiedColumns.map(_._1), analyzed)
      } else if (userSpecifiedColumns.length < analyzed.output.length) {
        throw QueryCompilationErrors.cannotCreateViewTooManyColumnsError(
          name, userSpecifiedColumns.map(_._1), analyzed)
      }
    }

    val schema = ViewHelper.aliasPlan(sparkSession, analyzed, userSpecifiedColumns).schema
    val columns = CatalogV2Util.structTypeToV2Columns(schema)
    val sourceTableFullName = extractSourceTable(originalText)

    val tableProperties = new java.util.HashMap[String, String]()
    comment.foreach(tableProperties.put("comment", _))
    properties.foreach { case (k, v) => tableProperties.put(k, v) }

    val deps = sourceTableFullName.map(name =>
      DependencyList.of(Dependency.table(name))
    ).orNull

    val tableInfo = new TableInfo.Builder()
      .withColumns(columns)
      .withProperties(tableProperties)
      .withTableType(TableSummary.METRIC_VIEW_TABLE_TYPE)
      .withViewDefinition(originalText)
      .withViewDependencies(deps)
      .build()

    tableCatalog.createTable(ident, tableInfo)
    Seq.empty
  }

  /**
   * Extracts the source table three-part name from the metric view YAML.
   * Only supports AssetSource (not SQLSource). Returns None if not extractable.
   */
  private def extractSourceTable(yaml: String): Option[String] = {
    try {
      val metricView = MetricViewFactory.fromYAML(yaml)
      metricView.from match {
        case asset: AssetSource => Some(asset.name)
        case _ => None
      }
    } catch {
      case _: Exception => None
    }
  }

  override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan = {
    copy(child = newChild)
  }
}

case class AlterMetricViewCommand(child: LogicalPlan, originalText: String)

object MetricViewHelper {
  def analyzeMetricViewText(
      session: SparkSession,
      name: TableIdentifier,
      viewText: String): LogicalPlan = {
    val analyzer = session.sessionState.analyzer
    // this metadata is used for analysis check, it'll be replaced during create/update with
    // more accurate information
    val tableMeta = CatalogTable(
      identifier = name,
      tableType = CatalogTableType.VIEW,
      storage = CatalogStorageFormat.empty,
      schema = new StructType(),
      viewOriginalText = Some(viewText),
      viewText = Some(viewText))
    val metricViewNode = MetricViewPlanner.planWrite(
      tableMeta, viewText, session.sessionState.sqlParser)
    val analyzed = analyzer.executeAndCheck(metricViewNode, new QueryPlanningTracker)
    ViewHelper.verifyTemporaryObjectsNotExists(isTemporary = false, name, analyzed, Seq.empty)
    analyzed
  }
}
