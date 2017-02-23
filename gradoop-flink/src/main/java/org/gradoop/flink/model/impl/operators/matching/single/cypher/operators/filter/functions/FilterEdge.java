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

package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.filter.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;

import java.util.HashMap;
import java.util.Map;

/**
 * Filters an Edge by a given predicate
 */
public class FilterEdge implements FilterFunction<Edge> {

  /**
   * Filter predicate
   */
  private final CNF predicates;

  /**
   * Mapping of variables to edges
   */
  private final Map<String, GraphElement> mapping;

  /**
   * Variable assigned to the edge
   */
  private final String edgeVariable;

  /**
   * Creates a new UDF
   *
   * @param edgeVariable variable assigned to the edge
   * @param predicates filter predicates
   */
  public FilterEdge(String edgeVariable, CNF predicates) {
    this.predicates = predicates;
    this.edgeVariable = edgeVariable;
    this.mapping = new HashMap<>();
  }

  @Override
  public boolean filter(Edge edge) throws Exception {
    mapping.put(edgeVariable, edge);
    return predicates.evaluate(mapping);
  }
}
