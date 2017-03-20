/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.filter;

import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.PhysicalOperator;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.filter.functions.FilterAndProjectTriple;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.Triple;

import java.util.List;
import java.util.Map;

/**
 * Filters a set of {@link Triple} objects based on a specified predicate. Additionally, the
 * operator projects all property values to the output {@link Embedding} that are specified in the
 * given {@code projectionPropertyKeys}.
 *
 * Triple -> Embedding( [PropertyEntry(SourceVertexId)], [PropertyEntry(EdgeId)], PropertyEntry
 * (TargetVertexId)])
 *
 * Example:
 *
 * Given a Triple
 * (
 *   p1: SourceVertex(0, "Person", {name:"Alice", age:23, location: "Sweden"}),
 *   e1: Edge(1, "knows", {}),
 *   p2: TargetVertex(2, "Person", {name:"Bob", age:23})
 * ),
 * a predicate "p1.name = "Alice" AND p1.age <= p2.age" and
 * projection property keys {p1: [name, location], e1: [], p2: [name, location] the operator creates
 * an {@link Embedding}:
 *
 * ([IdEntry(0),IdEntry(1),IdEntry(2)],[PropertyEntry("Alice"),PropertyEntry("Sweden"),
 * PropertyEntry("Bob"),PropertyEntry(NULL)])
 */
public class FilterAndProjectTriples implements PhysicalOperator {
  /**
   * Input vertices
   */
  private final DataSet<Triple> input;
  /**
   * Variable assigned to the source vertex
   */
  private final String sourceVariable;
  /**
   * Variable assigned to the edge
   */
  private final String edgeVariable;
  /**
   * Variable assigned to the target vertex
   */
  private final String targetVariable;
  /**
   * Predicates in conjunctive normal form
   */
  private final CNF predicates;
  /**
   * Property keys used for projection
   */
  private final Map<String, List<String>> projectionPropertyKeys;
  /**
   * Match Strategy used for Vertices
   */
  private final MatchStrategy vertexMatchStrategy;

  /**
   * New vertex filter operator
   *
   * @param input Candidate vertices
   * @param sourceVariable Variable assigned to the vertex
   * @param edgeVariable Variable assigned to the vertex
   * @param targetVariable Variable assigned to the vertex
   * @param predicates Predicates used to filter vertices
   * @param projectionPropertyKeys Property keys used for projection
   * @param vertexMatchStrategy Vertex match strategy
   */
  public FilterAndProjectTriples(DataSet<Triple> input, String sourceVariable, String edgeVariable,
    String targetVariable, CNF predicates, Map<String, List<String>> projectionPropertyKeys,
    MatchStrategy vertexMatchStrategy) {
    this.input = input;
    this.sourceVariable = sourceVariable;
    this.edgeVariable = edgeVariable;
    this.targetVariable = targetVariable;
    this.predicates = predicates;
    this.projectionPropertyKeys = projectionPropertyKeys;
    this.vertexMatchStrategy = vertexMatchStrategy;
  }

  @Override
  public DataSet<Embedding> evaluate() {
    return input.flatMap(
      new FilterAndProjectTriple(
        sourceVariable,
        edgeVariable,
        targetVariable,
        predicates,
        projectionPropertyKeys,
        vertexMatchStrategy
      )
    ).name(
      "FilterAndProjectTriples(" +
        sourceVariable + "-[" + targetVariable + "]->" + targetVariable +
      ")"
    );
  }
}
