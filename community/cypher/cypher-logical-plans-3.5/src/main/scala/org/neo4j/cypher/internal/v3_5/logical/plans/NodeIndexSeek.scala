/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.v3_5.logical.plans

import org.opencypher.v9_0.util.attribution.IdGen
import org.opencypher.v9_0.expressions.{Expression, LabelToken, PropertyKeyToken}

/**
  * For every node with the given label and property values, produces one row with that node.
  */
case class NodeIndexSeek(idName: String,
                         label: LabelToken,
                         propertyKeys: Seq[PropertyKeyToken],
                         valueExpr: QueryExpression[Expression],
                         argumentIds: Set[String])
                        (implicit idGen: IdGen) extends IndexLeafPlan(idGen) {

  override val availableSymbols: Set[String] = argumentIds + idName
}
