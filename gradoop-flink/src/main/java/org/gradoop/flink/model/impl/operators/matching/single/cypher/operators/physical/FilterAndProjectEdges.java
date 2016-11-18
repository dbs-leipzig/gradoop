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
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.functions.FilterAndProjectEdge;

import java.util.List;

/**
 * Filters a List of edges by predicates and projects the remaining elements to the
 * specified properties
 */
public class FilterAndProjectEdges implements PhysicalOperator {
  /**
   * The input data set
   */
  private final DataSet<Edge> input;
  /**
   * Predicate used for filtering in CNF
   */
  private final CNF predicates;
  /**
   * Holds a list of property keys for every embedding entry
   * The specified properties will be kept in the projection
   */
  private final List<String> propertyKeys;

  /**
   * Create a new edge filter and project function
   * @param input the vertex data set
   * @param predicates filter predicates
   * @param propertyKeys projection properties
   */
  public FilterAndProjectEdges(DataSet<Edge> input, CNF predicates, List<String> propertyKeys) {
    this.input = input;
    this.predicates = predicates;
    this.propertyKeys = propertyKeys;
  }

  @Override
  public DataSet<Embedding> evaluate() {
    return input.flatMap(
      new FilterAndProjectEdge(predicates, propertyKeys)
    );
  }
}
