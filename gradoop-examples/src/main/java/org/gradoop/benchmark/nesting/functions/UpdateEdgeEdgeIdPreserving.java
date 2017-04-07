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

package org.gradoop.benchmark.nesting.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Updates an EPGM edge with the given target vertex identifier.
 *
 * @param <E> EPGM edge type
 * @param <K> Import Edge/Vertex identifier type
 */
@FunctionAnnotation.ReadFieldsFirst("f2")
@FunctionAnnotation.ForwardedFields("f1 -> f0")
@FunctionAnnotation.ForwardedFieldsSecond("f2->targetId")
public class UpdateEdgeEdgeIdPreserving<E extends Edge, K extends Comparable<K>>
  implements JoinFunction<Tuple3<K, K, E>, Tuple2<K, GradoopId>, Tuple2<K, E>> {

  /**
   * Reusable element
   */
  private final Tuple2<K, E> reusable;

  /**
   * Default constructor
   */
  public UpdateEdgeEdgeIdPreserving() {
    reusable = new Tuple2<K, E>();
  }

  /**
   * Updates the target vertex identifier of the given EPGM edge.
   *
   * @param targetIdEdgePair import target id and EPGM edge
   * @param vertexIdPair     import target vertex id and EPGM vertex id
   * @return EPGM edge with updated target vertex id
   * @throws Exception
   */
  @Override
  public Tuple2<K, E> join(Tuple3<K, K, E> targetIdEdgePair,
    Tuple2<K, GradoopId> vertexIdPair) throws Exception {
    reusable.f0 = targetIdEdgePair.f1;
    reusable.f1 = targetIdEdgePair.f2;
    reusable.f1.setTargetId(vertexIdPair.f1);
    return reusable;
  }
}
