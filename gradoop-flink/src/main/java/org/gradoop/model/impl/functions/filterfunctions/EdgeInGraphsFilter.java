/*
 * This file is part of gradoop.
 *
 * gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.model.impl.functions.filterfunctions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.graph.Edge;
import org.gradoop.model.api.EdgeData;

import java.util.List;

/**
 * Checks if an edge is contained in at least one of the given logical
 * graphs.
 *
 * @param <ED> edge data type
 */
public class EdgeInGraphsFilter<ED extends EdgeData> implements
  FilterFunction<Edge<Long, ED>> {

  /**
   * Graph identifiers
   */
  private final List<Long> identifiers;

  /**
   * Creates a filter
   *
   * @param identifiers graph identifiers for containment check
   */
  public EdgeInGraphsFilter(List<Long> identifiers) {
    this.identifiers = identifiers;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean filter(Edge<Long, ED> e) throws Exception {
    boolean vertexInGraph = false;
    if (e.getValue().getGraphCount() > 0) {
      for (Long graph : e.getValue().getGraphs()) {
        if (identifiers.contains(graph)) {
          vertexInGraph = true;
          break;
        }
      }
    }
    return vertexInGraph;
  }
}

