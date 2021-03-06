/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.cypher.internal.executionplan.builders

import org.neo4j.cypher.internal.executionplan.{PartiallySolvedQuery, PlanBuilder}
import org.neo4j.cypher.internal.pipes.{ExtractPipe, EagerAggregationPipe, Pipe}
import org.neo4j.cypher.internal.commands.{Entity, AggregationExpression}

class AggregationBuilder extends PlanBuilder {
  def apply(v1: (Pipe, PartiallySolvedQuery)): (Pipe, PartiallySolvedQuery) = v1 match {
    case (p, q) => {
      val aggregationExpressions = q.aggregation.map(_.token)
      val keyExpressions = q.returns.map(_.token.expression).filterNot(_.containsAggregate)

      val extractor = new ExtractPipe(p, keyExpressions)
      val aggregator = new EagerAggregationPipe(extractor, keyExpressions, aggregationExpressions)

      val notKeyAndNotAggregate = q.returns.map(_.token.expression).filterNot(keyExpressions.contains)

      val resultPipe = if (notKeyAndNotAggregate.isEmpty) {
        aggregator
      } else {

        val rewritten = notKeyAndNotAggregate.map(e => {
          e.rewrite {
            case x: AggregationExpression => Entity(x.identifier.name)
            case x => x
          }
        })

        new ExtractPipe(aggregator, rewritten)
      }

      (resultPipe, q.copy(
        aggregation = q.aggregation.map(_.solve),
        aggregateQuery = q.aggregateQuery.solve,
        extracted = true
      ))
    }
  }

  def isDefinedAt(x: (Pipe, PartiallySolvedQuery)): Boolean = x match {
    case (p, q) =>
      q.aggregateQuery.token &&
        q.aggregateQuery.unsolved &&
        q.readyToAggregate
  }

  def priority: Int = PlanBuilder.Aggregation
}