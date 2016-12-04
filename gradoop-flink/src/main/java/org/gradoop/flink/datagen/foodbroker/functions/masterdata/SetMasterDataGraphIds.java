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

package org.gradoop.flink.datagen.foodbroker.functions.masterdata;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.Vertex;

import java.util.Map;

/**
 * Sets the graph ids for each master data vertex. The ids a taken from a broadcasted map.
 */
public class SetMasterDataGraphIds extends RichMapFunction<Vertex, Vertex> {
  /**
   * Hashmap from a gradoop id to a gradoop id set, which contains all graph ids the key id is
   * part of.
   */
  Map<GradoopId, GradoopIdSet> map;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    map = getRuntimeContext().<Map<GradoopId, GradoopIdSet>>getBroadcastVariable("graphIds").get(0);
  }

  @Override
  public Vertex map(Vertex vertex) throws Exception {
    vertex.setGraphIds(map.get(vertex.getId()));
    return vertex;
  }
}
