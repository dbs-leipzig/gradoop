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

package org.gradoop.flink.datagen.foodbroker.functions;

import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdList;

import java.util.Map;

/**
 * Creates a map from a gradoop id to all graph ids in which the corresponding graph element is part
 * of.
 */
public class GraphIdsMapFromTuple implements
  GroupReduceFunction<Tuple2<GradoopId, GradoopIdList>, Map<GradoopId, GradoopIdList>> {

  @Override
  public void reduce(Iterable<Tuple2<GradoopId, GradoopIdList>> iterable,
    Collector<Map<GradoopId, GradoopIdList>> collector)
    throws Exception {
    Map<GradoopId, GradoopIdList> map = Maps.newHashMap();
    GradoopIdList graphIds;
    // fill map with element and the graph ids
    for (Tuple2<GradoopId, GradoopIdList> tuple : iterable) {
      // if the element is already part of one graph
      if (map.containsKey(tuple.f0)) {
        graphIds = map.get(tuple.f0);
        graphIds.addAll(tuple.f1);
      } else {
        graphIds = tuple.f1;
      }
      map.put(tuple.f0, graphIds);
    }
    collector.collect(map);

  }
}
