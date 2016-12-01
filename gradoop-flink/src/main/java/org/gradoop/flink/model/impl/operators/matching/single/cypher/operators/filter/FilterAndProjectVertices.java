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
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.filter.functions.FilterAndProjectVertex;


import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.PhysicalOperator;

import java.util.List;

/**
 * Filters a List of vertices by predicates and projects the remaining elements to the
 * specified properties
 */
public class FilterAndProjectVertices implements PhysicalOperator {
  /**
   * vertex data set
   */
  private final DataSet<Vertex> input;
  /**
   * Predicate used for filtering in CNF
   */
  private final CNF predicates;
  /**
   * Specifies properties that will be kept in the projection
   */
  private final List<String> propertyKeys;

  /**
   * Create a new vertex filter and project function
   * @param input input data set
   * @param predicates filter predicates
   * @param propertyKeys projection properties
   */
  public FilterAndProjectVertices(DataSet<Vertex> input, CNF predicates,
    List<String> propertyKeys) {

    this.input = input;
    this.predicates = predicates;
    this.propertyKeys = propertyKeys;
  }

  @Override
  public DataSet<Embedding> evaluate() {
    return input.flatMap(
      new FilterAndProjectVertex(predicates, propertyKeys)
    );
  }
}
