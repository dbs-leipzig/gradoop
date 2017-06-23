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
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;

/**
 * Filters vertices by a given predicate
 */
public class FilterVertex implements FilterFunction<Vertex> {

  /**
   * Filter predicate
   */
  private final CNF predicates;

  /**
   * Creates a new UDF
   *
   * @param predicates filter predicates
   */
  public FilterVertex(CNF predicates) {
    this.predicates = predicates;
  }

  @Override
  public boolean filter(Vertex vertex) throws Exception {
    return predicates.evaluate(vertex);
  }
}
