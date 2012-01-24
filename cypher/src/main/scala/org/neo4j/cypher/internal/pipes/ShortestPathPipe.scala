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
package org.neo4j.cypher.internal.pipes

import org.neo4j.kernel.Traversal
import org.neo4j.cypher.SyntaxException
import java.lang.String
import collection.Seq
import org.neo4j.cypher.symbols.{NodeType, Identifier, PathType}
import org.neo4j.cypher.commands.{Predicate, ShortestPath}
import java.lang.{Iterable => JIterable}
import org.neo4j.graphdb._
import scala.collection.JavaConverters._

/**
 * Shortest pipe inserts a single shortest path between two already found nodes
 *
 * It's also the base class for all shortest paths
 */
abstract class ShortestPathPipe(source: Pipe, ast: ShortestPath) extends PipeWithSource(source) {
  private def startName = ast.start
  private def endName = ast.end
  private def relType = ast.relType
  private def dir = ast.dir
  private def maxDepth = ast.maxDepth
  def optional = ast.optional
  def pathName = ast.pathName

  def createResults[U](params: Map[String, Any]): Traversable[Map[String, Any]] = source.createResults(params).flatMap(m => {
    val (start, end) = getStartAndEnd(m)
    val depth: Int = maxDepth.getOrElse(15)

    findResult(createExpander(m), start, end,  depth, m)
  })

  class PredicateExpander(inner: RelationshipExpander, predicate: Predicate, m: Map[String,Any]) extends RelationshipExpander {
    def expand(node: Node):JIterable[Relationship] = inner.expand(node).asScala.filter(r=> predicate.isMatch(m)).asJava

    def reversed() = new PredicateExpander(inner.reversed(), predicate, m)
  }

  private def getStartAndEnd[U](m: Map[String, Any]): (Node, Node) = {
    val err = (n: String) => throw new SyntaxException("To find a shortest path, both ends of the path need to be provided. Couldn't find `" + n + "`")

    val start = m.getOrElse(startName, err(startName)).asInstanceOf[Node]
    val end = m.getOrElse(endName, err(endName)).asInstanceOf[Node]
    (start, end)
  }

  private def createExpander(m:Map[String,Any]): RelationshipExpander = {
    val expander = relType match {
      case None => Traversal.expanderForAllTypes(dir)
      case Some(typeName) => Traversal.expanderForTypes(DynamicRelationshipType.withName(typeName), dir)
    }

    new PredicateExpander(expander, ast.predicate, m)
  }

  def dependencies: Seq[Identifier] = Seq(Identifier(startName, NodeType()), Identifier(endName, NodeType()))

  protected def findResult[U](expander: RelationshipExpander, start: Node, end: Node, depth: Int, m: Map[String, Any]):Traversable[Map[String, Any]]

  val symbols = source.symbols.add(Identifier(pathName, PathType()))

  override def executionPlan(): String = source.executionPlan() + "\r\n" + "ShortestPath(" + ast + ")"
}

// My daughters wrote this when I left the laptop open 2011-09-22. Now it belongs here.
//      lola
//      nina
//      wilma
//      mimi

//      nina
//      andres tykör alla om andres är snel han jör mango till sina barn han tröstar sina barn när dom är lesna han -
//      jör god mat han läsör bok och han har jet mej en ajpäd
//      lola
//      jag älskar andres andres pappa är rar du är fin du har fint namn

