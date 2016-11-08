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

package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.physical;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.Embedding;
import org.s1ck.gdl.model.cnf.CNF;

/**
 * Filters a List of Edges by predicates and projects the remaining edges to the specified
 * properties
 * Returns Embedding with three columns IdEntry(sourceID), IdEntry(Edge), IdEntry(targetId)
 */
public class FilterEdges implements PhysicalOperator {
  /**
   * Input Edges
   */
  private final DataSet<Edge> input;
  /**
   * Predicates in conjunctive normal form
   */
  private final CNF predicates;


  /**
   * New edge filter operator
   * @param input Candidate edges
   * @param predicates Predicates used to filter edges
   */
  public FilterEdges(DataSet<Edge> input, CNF predicates) {
    this.input = input;
    this.predicates = predicates;
  }

  @Override
  public DataSet<Embedding> evaluate() {

    return null;
  }
}
