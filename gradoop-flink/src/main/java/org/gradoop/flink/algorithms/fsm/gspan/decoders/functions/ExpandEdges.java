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

package org.gradoop.flink.algorithms.fsm.gspan.decoders.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.impl.id.GradoopId;

import java.util.ArrayList;

/**
 * (GraphId, [EPGMEdge,..], [EPGMEdge,..]) => [EPGMEdge,..]
 * @param <G> graph type
 */
public class ExpandEdges<G extends EPGMGraphHead>
  implements FlatMapFunction
  <Tuple3<G, ArrayList<Tuple2<GradoopId, Integer>>,
    ArrayList<Tuple3<GradoopId, GradoopId, Integer>>>,
    Tuple4<GradoopId, GradoopId, GradoopId, Integer>> {

  @Override
  public void flatMap(
    Tuple3<G, ArrayList<Tuple2<GradoopId, Integer>>,
      ArrayList<Tuple3<GradoopId, GradoopId, Integer>>> graph,
    Collector<Tuple4<GradoopId, GradoopId, GradoopId, Integer>> collector
  ) throws Exception {

    for (Tuple3<GradoopId, GradoopId, Integer> edge : graph.f2) {
      collector.collect(new Tuple4<>(
        graph.f0.getId(), edge.f0, edge.f1, edge.f2));
    }
  }
}
