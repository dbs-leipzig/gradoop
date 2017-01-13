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
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.EmbeddingMetaData;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.PhysicalOperator;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.filter.functions.FilterAndProjectEdge;

/**
 * Filters a set of EPGM {@link Edge} objects based on a specified predicate. Additionally, the
 * operator projects all property values to the output {@link Embedding} that are specified in the
 * given {@link EmbeddingMetaData}.
 *
 * Edge -> Embedding(
 *  [IdEntry(SourceId),IdEntry(EdgeId),IdEntry(TargetId)],
 *  [PropertyEntry(v1),PropertyEntry(v2)]
 * )
 *
 * Example:
 *
 * Given an Edge(0, 1, 2, "friendOf", {since:2017, weight:23}), a predicate "weight = 23" and a
 * mapping between property keys and column indices {weight:0,isValid:1} the operator creates
 * an {@link Embedding}:
 *
 * ([IdEntry(1),IdEntry(0),IdEntry(2)],[PropertyEntry(23),PropertyEntry(NULL)])
 */
public class FilterAndProjectEdges implements PhysicalOperator {
  /**
   * Input graph elements
   */
  private final DataSet<Edge> input;
  /**
   * Predicates in conjunctive normal form
   */
  private final CNF predicates;
  /**
   * Meta data describing the embedding used for filtering
   */
  private final EmbeddingMetaData metaData;

  /**
   * New edge filter operator
   *
   * @param input Candidate edges
   * @param predicates Predicates used to filter edges
   * @param metaData Meta data describing the embedding used for filtering
   */
  public FilterAndProjectEdges(DataSet<Edge> input, CNF predicates, EmbeddingMetaData metaData) {
    this.input = input;
    this.predicates = predicates;
    this.metaData = metaData;
  }

  @Override
  public DataSet<Embedding> evaluate() {
    return input.flatMap(new FilterAndProjectEdge(predicates, metaData));
  }
}
