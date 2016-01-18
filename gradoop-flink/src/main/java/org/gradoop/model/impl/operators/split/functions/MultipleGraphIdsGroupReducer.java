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

package org.gradoop.model.impl.operators.split.functions;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.id.GradoopId;

import java.util.ArrayList;
import java.util.List;

/**
 * groupReduce each group of vertices into a single vertex, whose graphId set
 * contains all graphs of each origin vertex
 */
public class MultipleGraphIdsGroupReducer implements GroupReduceFunction
    <Tuple2<GradoopId, GradoopId>, Tuple2<GradoopId, List<GradoopId>>> {

  @Override
  public void reduce(
    Iterable<Tuple2<GradoopId, GradoopId>> iterable,
    Collector<Tuple2<GradoopId, List<GradoopId>>> collector) {

    boolean first = true;
    GradoopId vertexId = null;
    List<GradoopId> newGraphIds = Lists.newArrayList();

    for (Tuple2<GradoopId, GradoopId> tuple : iterable) {
      if (first) {
        vertexId = tuple.f0;
        first = false;
      }
      newGraphIds.add(tuple.f1);
    }
    collector.collect(new Tuple2<>(vertexId, newGraphIds));
  }
}
