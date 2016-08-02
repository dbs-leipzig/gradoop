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

package org.gradoop.flink.model.impl.operators.subgraph.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Maps an edge  tuples 4 containing the id of this edge, the id of its
 * source and target and a graph this edge is contained in. One tuple per graph.
 *
 * @param <E> epgm edge type
 */

@FunctionAnnotation.ReadFields("graphIds")
@FunctionAnnotation.ForwardedFields("id->f0;sourceId->f1;targetId->f2")
public class IdSourceTargetGraphTuple<E extends Edge>
  implements FlatMapFunction<
  E, Tuple4<GradoopId, GradoopId, GradoopId, GradoopId>> {

  @Override
  public void flatMap(
    E edge,
    Collector<Tuple4<GradoopId, GradoopId, GradoopId, GradoopId>> collector) {

    for (GradoopId graphId : edge.getGraphIds()) {
      collector.collect(new Tuple4<>(
        edge.getId(),
        edge.getSourceId(),
        edge.getTargetId(),
        graphId));
    }

  }
}
