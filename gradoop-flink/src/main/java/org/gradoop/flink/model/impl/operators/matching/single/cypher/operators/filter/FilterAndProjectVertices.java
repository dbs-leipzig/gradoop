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
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.EmbeddingMetaData;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.PhysicalOperator;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.filter.functions.FilterAndProjectVertex;

/**
 * Filters a set of EPGM {@link Vertex} objects based on a specified predicate. Additionally, the
 * operator projects all property values to the output {@link Embedding} that are specified in the
 * given {@link EmbeddingMetaData}.
 *
 * Vertex -> Embedding( [IdEntry(VertexId)], [PropertyEntry(v1),PropertyEntry(v2)])
 *
 * Example:
 *
 * Given a Vertex(0, "Person", {name:"Alice", age:23}), a predicate "age = 23" and a mapping between
 * property keys and column indices {age:0,name:1,location:2} the operator creates an
 * {@link Embedding}:
 *
 * ([IdEntry(0)],[PropertyEntry(23),PropertyEntry(Alice),PropertyEntry(NULL)])
 */
public class FilterAndProjectVertices implements PhysicalOperator {
  /**
   * Input vertices
   */
  private final DataSet<Vertex> input;
  /**
   * Predicates in conjunctive normal form
   */
  private final CNF predicates;
  /**
   * Meta data describing the vertex used for filtering
   */
  private final EmbeddingMetaData metaData;

  /**
   * New vertex filter operator
   *
   * @param input Candidate vertices
   * @param predicates Predicates used to filter vertices
   * @param metaData Meta data describing the embedding used for filtering
   */
  public FilterAndProjectVertices(DataSet<Vertex> input, CNF predicates,
    EmbeddingMetaData metaData) {
    this.input = input;
    this.predicates = predicates;
    this.metaData = metaData;
  }

  @Override
  public DataSet<Embedding> evaluate() {
    return input.flatMap(new FilterAndProjectVertex(predicates, metaData));
  }
}
