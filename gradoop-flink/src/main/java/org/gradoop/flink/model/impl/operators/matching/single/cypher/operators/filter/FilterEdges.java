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
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.filter.functions.FilterEdge;

/**
 * Filters a List of edges by predicates
 * Edge -> Embedding(IdEntry(SrcID), IdEntry(Edge), IdEntry(TargetID))
 * No properties will be adopted.
 */
public class FilterEdges implements PhysicalOperator {
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
   * @param input Candidate edges
   * @param predicates Predicates used to filter edges
   * @param metaData Meta data describing the embedding used for filtering
   */
  public FilterEdges(DataSet<Edge> input, CNF predicates, EmbeddingMetaData metaData) {
    this.input = input;
    this.predicates = predicates;
    this.metaData = metaData;
  }

  @Override
  public DataSet<Embedding> evaluate() {
    return input.flatMap(new FilterEdge(predicates, metaData));
  }
}
