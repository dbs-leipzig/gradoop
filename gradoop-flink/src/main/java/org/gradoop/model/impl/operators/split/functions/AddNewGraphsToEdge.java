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
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.impl.id.GradoopId;

import java.util.List;

/**
 * Adds new graph id's to the edge if source and target vertex are part of
 * the same graph. Filters all edges between graphs.
 *
 * @param <E> EPGM edge Type
 */
public class AddNewGraphsToEdge<E extends EPGMEdge>
  implements FlatMapFunction<Tuple3<E, List<GradoopId>, List<GradoopId>>, E> {

  @Override
  public void flatMap(
    Tuple3<E, List<GradoopId>, List<GradoopId>> tuple3,
    Collector<E> collector) {
    List<GradoopId> sourceGraphs = tuple3.f1;
    List<GradoopId> targetGraphs = tuple3.f2;
    List<GradoopId> graphsToBeAdded = Lists.newArrayList();
    boolean filter = false;
    for (GradoopId id : sourceGraphs) {
      if (targetGraphs.contains(id)) {
        graphsToBeAdded.add(id);
        filter = true;
      }
    }
    E edge = tuple3.f0;
    edge.getGraphIds().addAll(graphsToBeAdded);
    if (filter) {
      collector.collect(edge);
    }
  }
}
