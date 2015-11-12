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

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.model.api.EPGMVertex;

import java.util.List;

/**
 * Checks if a vertex is contained in at least one of the given logical
 * graphs.
 *
 * Graph identifiers are read from a broadcast set.
 *
 * @param <VD> EPGM vertex type
 */
public class VertexInGraphsFilterWithBC<VD extends EPGMVertex>
  extends RichFilterFunction<VD> {

  /**
   * Name of the broadcast variable which is accessed by this function.
   */
  public static final String BC_IDENTIFIERS = "bc.identifiers";

  /**
   * Graph identifiers
   */
  private List<Long> identifiers;

  /**
   * {@inheritDoc}
   */
  @Override
  public void open(Configuration parameters) throws Exception {
    identifiers = getRuntimeContext().getBroadcastVariable(BC_IDENTIFIERS);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean filter(VD vertex) throws Exception {
    boolean vertexInGraph = false;
    if (vertex.getGraphCount() > 0) {
      for (Long graph : vertex.getGraphIds()) {
        if (identifiers.contains(graph)) {
          vertexInGraph = true;
          break;
        }
      }
    }
    return vertexInGraph;
  }
}

