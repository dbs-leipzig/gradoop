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

package org.gradoop.flink.io.impl.hbase.functions;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;

import java.util.Set;

/**
 * Co-group (vertex-data) with (vertex-id, [out-edge]) to
 * (vertex-data, [out-edge]).
 *
 * Forwarded fields first:
 *
 * * -> f0: vertex data
 *
 * Forwarded fields second:
 *
 * f1: [out-edge]
 *
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
@FunctionAnnotation.ForwardedFieldsFirst("*->f0")
@FunctionAnnotation.ForwardedFieldsSecond("f1")
public class BuildVertexDataWithEdges<V extends Vertex, E extends Edge>
  implements CoGroupFunction<V, Tuple2<GradoopId, Set<E>>, Tuple2<V, Set<E>>> {
  /**
   * Reduce object instantiations
   */
  private final Tuple2<V, Set<E>> reuseTuple = new Tuple2<>();

  @Override
  public void coGroup(Iterable<V> vertexIterable,
    Iterable<Tuple2<GradoopId, Set<E>>> outEdgesIterable,
    Collector<Tuple2<V, Set<E>>> collector) throws Exception {
    // read vertex from left group
    V vertex = vertexIterable.iterator().next();
    Set<E> outgoingEdgeData = null;

    // read outgoing edge from right group (may be empty)
    for (Tuple2<GradoopId, Set<E>> oEdges : outEdgesIterable) {
      outgoingEdgeData = oEdges.f1;
    }
    reuseTuple.f0 = vertex;
    reuseTuple.f1 = outgoingEdgeData;
    collector.collect(reuseTuple);
  }
}
