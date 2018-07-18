/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.filter;

import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.PhysicalOperator;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.filter.functions.FilterAndProjectTriple;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Triple;

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
   * Operator name used for Flink operator description
   */
  private String name;

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
    this.setName("FilterAndProjectTriples");
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
    ).name(getName());
  }

  @Override
  public void setName(String newName) {
    this.name = newName;
  }

  @Override
  public String getName() {
    return this.name;
  }
}
