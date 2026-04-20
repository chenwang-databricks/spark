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
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.util.ArrayImplicits._

/**
 * Physical plan node for `DROP VIEW` against a non-session V2 catalog when the resolved
 * target is a metric view. Metric views are stored as tables on the V2 catalog (via
 * `createTable(Identifier, TableInfo)`) but are conceptually views, so `DROP VIEW` is the
 * supported DDL for them. The strategy layer is responsible for routing only metric-view
 * drops to this exec; this class only performs the drop and honors `IF EXISTS` semantics.
 */
case class DropMetricViewExec(
    catalog: TableCatalog,
    ident: Identifier,
    ifExists: Boolean) extends LeafV2CommandExec {

  override def run(): Seq[InternalRow] = {
    if (catalog.tableExists(ident)) {
      catalog.dropTable(ident)
    } else if (!ifExists) {
      throw QueryCompilationErrors.noSuchTableError(
        (catalog.name() +: ident.namespace() :+ ident.name()).toImmutableArraySeq)
    }
    Seq.empty
  }

  override def output: Seq[Attribute] = Seq.empty
}
