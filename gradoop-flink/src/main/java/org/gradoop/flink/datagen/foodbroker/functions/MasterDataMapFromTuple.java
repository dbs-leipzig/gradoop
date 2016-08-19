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

import java.util.Map;

/**
 * Returns a map from each gradoop id to the object.
 */
public class MasterDataMapFromTuple<T>
  implements GroupReduceFunction<Tuple2<GradoopId, T>,
    Map<GradoopId, T>> {

  @Override
  public void reduce(Iterable<Tuple2<GradoopId, T>> iterable,
    Collector<Map<GradoopId, T>> collector) throws
    Exception {
    Map<GradoopId, T> map = Maps.newHashMap();
    for(Tuple2<GradoopId, T> tuple : iterable) {
      map.put(tuple.f0, tuple.f1);
    }
    collector.collect(map);
  }
}
