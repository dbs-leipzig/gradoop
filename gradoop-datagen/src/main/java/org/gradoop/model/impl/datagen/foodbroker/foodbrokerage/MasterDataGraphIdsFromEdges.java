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

package org.gradoop.model.impl.datagen.foodbroker.foodbrokerage;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.datagen.foodbroker.config.Constants;
import org.gradoop.model.impl.id.GradoopIdSet;

import java.util.List;

/**
 * Map which adds all related graph ids to the master data vertices.
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class MasterDataGraphIdsFromEdges<G extends EPGMGraphHead,
  V extends EPGMVertex, E extends EPGMEdge> extends RichMapFunction<V, V> {

  /**
   * list of all edges
   */
  private List<E> edges;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    edges = getRuntimeContext().getBroadcastVariable(Constants.EDGES);
  }

  @Override
  public V map(V v) throws Exception {
    GradoopIdSet set = new GradoopIdSet();
    for (E edge : edges) {
      if (edge.getTargetId().equals(v.getId())) {
        set.addAll(edge.getGraphIds());
      }
    }
    v.setGraphIds(set);
    return v;
  }
}
