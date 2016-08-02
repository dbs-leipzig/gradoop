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

package org.gradoop.flink.model.impl.functions.graphcontainment;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Takes a vertex and creates one tuple 2 of this vertex and a graph id per
 * graph the vertex is contained in.
 *
 * @param <V> epgm vertex type
 */
public class PairVertexWithGraphs<V extends Vertex>
  implements FlatMapFunction<V, Tuple2<V, GradoopId>> {

  @Override
  public void flatMap(V v, Collector<Tuple2<V, GradoopId>> collector) throws
    Exception {
    for (GradoopId graphId : v.getGraphIds()) {
      collector.collect(new Tuple2<>(v, graphId));
    }
  }
}
