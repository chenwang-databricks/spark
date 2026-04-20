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

package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.catalog.{CatalogPlugin, Identifier, TableCatalog, TableSummary}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.util.ArrayImplicits._

/**
 * Physical plan node for `DROP VIEW` against a non-session V2 catalog. Metric views are stored
 * as tables on the V2 catalog (via `createTable(Identifier, TableInfo)`) but they are
 * conceptually views, so `DROP VIEW` is the supported DDL for them. This exec only succeeds
 * when the resolved table is a metric view; for regular tables it throws the standard
 * "wrong command for object type" error directing the caller to `DROP TABLE` instead. If the
 * V2 catalog has no table at the identifier, behavior matches `DROP VIEW`'s `IF EXISTS` flag.
 */
case class DropMetricViewExec(
    catalog: CatalogPlugin,
    ident: Identifier,
    ifExists: Boolean) extends LeafV2CommandExec {

  override def run(): Seq[InternalRow] = {
    val tableCatalog = catalog.asInstanceOf[TableCatalog]
    if (!tableCatalog.tableExists(ident)) {
      if (!ifExists) {
        throw QueryCompilationErrors.noSuchTableError(
          (catalog.name() +: ident.namespace() :+ ident.name()).toImmutableArraySeq)
      }
      return Seq.empty
    }
    val table = tableCatalog.loadTable(ident)
    val tableType = Option(table.properties().get(TableCatalog.PROP_TABLE_TYPE)).orNull
    if (TableSummary.METRIC_VIEW_TABLE_TYPE.equals(tableType)) {
      tableCatalog.dropTable(ident)
    } else {
      val qualified = (catalog.name() +: ident.namespace() :+ ident.name()).mkString(".")
      throw QueryCompilationErrors.wrongCommandForObjectTypeError(
        operation = "DROP VIEW",
        requiredType = TableSummary.METRIC_VIEW_TABLE_TYPE,
        objectName = qualified,
        foundType = Option(tableType).getOrElse("TABLE"),
        alternative = "DROP TABLE"
      )
    }
    Seq.empty
  }

  override def output: Seq[Attribute] = Seq.empty
}
