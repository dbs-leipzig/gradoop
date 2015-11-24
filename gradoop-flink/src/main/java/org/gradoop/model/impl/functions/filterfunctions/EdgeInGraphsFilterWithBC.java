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
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIdSet;
import org.gradoop.model.impl.id.GradoopIds;

import java.util.List;

/**
 * Checks if an edge is contained in at least one of the given logical
 * graphs.
 *
 * Graph identifiers are read from a broadcast set.
 *
 * @param <ED> EPGM edge type
 */
public class EdgeInGraphsFilterWithBC<ED extends EPGMEdge>
  extends RichFilterFunction<ED> {

  /**
   * Name of the broadcast variable which is accessed by this function.
   */
  public static final String BC_IDENTIFIERS = "bc.identifiers";

  /**
   * Graph identifiers
   */
  private GradoopIdSet identifiers;

  /**
   * {@inheritDoc}
   */
  @Override
  public void open(Configuration parameters) throws Exception {
    List<GradoopId> gradoopIds = getRuntimeContext().getBroadcastVariable
      (BC_IDENTIFIERS);

    identifiers = new GradoopIdSet();

    for (GradoopId id : gradoopIds) {
      identifiers.add(id);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean filter(ED edge) throws Exception {
    boolean vertexInGraph = false;
    if (edge.getGraphCount() > 0) {
      for (GradoopId graph : edge.getGraphIds()) {
        if (identifiers.contains(graph)) {
          vertexInGraph = true;
          break;
        }
      }
    }
    return vertexInGraph;
  }
}

