package org.neo4j.cypher.commands

abstract class Rewriter {
  def apply(v: Expression): Expression = rewriteExpression(v)
  def apply(v: Predicate): Predicate = rewritePredicate(v)

  def rewriteExpression: Expression => Expression = x => x

  def rewritePredicate: Predicate => Predicate = x => x
}

