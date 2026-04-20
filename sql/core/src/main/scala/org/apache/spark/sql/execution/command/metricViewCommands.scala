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
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType, HiveTableRelation}
import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, View}
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, Dependency, DependencyList, TableCatalog, TableInfo, TableSummary}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.metricview.serde.MetricViewFactory
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
    val sourceTableNames = MetricViewHelper.collectTableDependencies(analyzed)

    // Mirror what the V1 path produces via ViewHelper.prepareTable: capture the
    // create-time catalog/namespace and SQL configs so the view text can be
    // re-parsed and resolved consistently when the metric view is loaded later.
    val viewProps = ViewHelper.generateViewProperties(
      properties, sparkSession,
      analyzed.schema.fieldNames, schema.fieldNames,
      SchemaUnsupported)

    // Describe this metric view's source and filter as table properties so
    // catalogs and tools can inspect them without re-parsing the YAML.
    val metricView = MetricViewFactory.fromYAML(originalText)
    val metricViewProps = metricView.getProperties

    val tableProperties = new java.util.HashMap[String, String]()
    comment.foreach(tableProperties.put(TableCatalog.PROP_COMMENT, _))
    viewProps.foreach { case (k, v) => tableProperties.put(k, v) }
    metricViewProps.foreach { case (k, v) => tableProperties.put(k, v) }

    val deps = if (sourceTableNames.nonEmpty) {
      DependencyList.of(sourceTableNames.map(Dependency.table): _*)
    } else {
      null
    }

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

  override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan = {
    copy(child = newChild)
  }
}

case class AlterMetricViewCommand(child: LogicalPlan, originalText: String)

object MetricViewHelper {

  /**
   * Walks the analyzed plan to collect direct table/view dependencies.
   * Stops recursion at relation leaf nodes and persistent View nodes so that only
   * direct (not transitive) dependencies are recorded.
   */
  private[execution] def collectTableDependencies(plan: LogicalPlan): Seq[String] = {
    val tables = scala.collection.mutable.ArrayBuffer.empty[String]
    def traverse(p: LogicalPlan): Unit = p match {
      case v: View if !v.isTempView =>
        tables += v.desc.identifier.unquotedString
      case r: DataSourceV2Relation if r.catalog.isDefined && r.identifier.isDefined =>
        val cat = r.catalog.get.name()
        val ns = r.identifier.get.namespace().mkString(".")
        val name = r.identifier.get.name()
        tables += s"$cat.$ns.$name"
      case r: HiveTableRelation =>
        tables += r.tableMeta.identifier.unquotedString
      case r: LogicalRelation if r.catalogTable.isDefined =>
        tables += r.catalogTable.get.identifier.unquotedString
      case other =>
        other.children.foreach(traverse)
        other.expressions.foreach(_.foreach {
          case s: SubqueryExpression => traverse(s.plan)
          case _ =>
        })
    }
    traverse(plan)
    tables.distinct.toSeq
  }

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
