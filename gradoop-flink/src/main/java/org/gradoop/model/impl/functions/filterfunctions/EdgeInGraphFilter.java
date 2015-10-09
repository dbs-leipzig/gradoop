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

/**
 * Checks if an edge is contained in the given graph.
 *
 * @param <ED> edge data type
 */
public class EdgeInGraphFilter<ED extends EdgeData> implements
  FilterFunction<Edge<Long, ED>> {

  /**
   * Graph identifier
   */
  private final long graphId;

  /**
   * Creates a filter
   *
   * @param graphId graphId for containment check
   */
  public EdgeInGraphFilter(long graphId) {
    this.graphId = graphId;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean filter(Edge<Long, ED> e) throws Exception {
    return (e.getValue().getGraphCount() > 0) &&
      e.getValue().getGraphs().contains(graphId);
  }
}

