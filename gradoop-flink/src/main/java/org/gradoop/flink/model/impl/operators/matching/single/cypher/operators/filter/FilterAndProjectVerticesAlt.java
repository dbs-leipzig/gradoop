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
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.PhysicalOperator;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.filter.functions.FilterVertex;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.project.functions.ProjectVertex;

import java.util.List;

/**
 * Alternative FilterAndProjectVertices using Filter + Map operators
 *
 * Filters a set of EPGM {@link Vertex} objects based on a specified predicate. Additionally, the
 * operator projects all property values to the output {@link Embedding} that are specified in the
 * given {@code projectionPropertyKeys}.
 *
 * Vertex -> Embedding( [IdEntry(VertexId)], [PropertyEntry(v1),PropertyEntry(v2)])
 *
 * Example:
 *
 * Given a Vertex(0, "Person", {name:"Alice", age:23}), a predicate "age = 23" and
 * projection property keys [name, location] the operator creates an
 * {@link Embedding}:
 *
 * ([IdEntry(0)],[PropertyEntry(Alice),PropertyEntry(NULL)])
 */
public class FilterAndProjectVerticesAlt implements PhysicalOperator {
  /**
   * Input vertices
   */
  private final DataSet<Vertex> input;
  /**
   * Predicates in conjunctive normal form
   */
  private final CNF predicates;
  /**
   * Property keys used for projection
   */
  private final List<String> projectionPropertyKeys;

  /**
   * Operator name used for Flink operator description
   */
  private String name;

  /**
   * New vertex filter operator
   *
   * @param input Candidate vertices
   * @param predicates Predicates used to filter vertices
   * @param projectionPropertyKeys Property keys used for projection
   */
  public FilterAndProjectVerticesAlt(DataSet<Vertex> input, CNF predicates,
    List<String> projectionPropertyKeys) {
    this.input = input;
    this.predicates = predicates;
    this.projectionPropertyKeys = projectionPropertyKeys;
    this.setName("FilterAndProjectVerticesAlt");
  }

  @Override
  public DataSet<Embedding> evaluate() {
    return input
      .filter(new FilterVertex(predicates))
      .name(getName())
      .map(new ProjectVertex(projectionPropertyKeys))
      .name(getName());

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
