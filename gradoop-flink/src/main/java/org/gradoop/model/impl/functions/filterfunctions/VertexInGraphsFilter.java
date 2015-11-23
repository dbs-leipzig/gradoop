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
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIdSet;

/**
 * Checks if a vertex is contained in at least one of the given logical
 * graphs.
 *
 * @param <VD> EPGM vertex type
 */
public class VertexInGraphsFilter<VD extends EPGMVertex>
  implements FilterFunction<VD> {

  /**
   * Graph identifiers
   */
  private final GradoopIdSet identifiers;

  /**
   * Creates a filter
   *
   * @param identifiers graph identifiers for containment check
   */
  public VertexInGraphsFilter(GradoopIdSet identifiers) {
    this.identifiers = identifiers;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean filter(VD vertex) throws Exception {
    boolean vertexInGraph = false;
    if (vertex.getGraphCount() > 0) {
      for (GradoopId graph : vertex.getGraphIds()) {
        if (identifiers.contains(graph)) {
          vertexInGraph = true;
          break;
        }
      }
    }
    return vertexInGraph;
  }
}

