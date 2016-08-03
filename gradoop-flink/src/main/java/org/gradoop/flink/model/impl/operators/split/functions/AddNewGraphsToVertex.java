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

package org.gradoop.flink.model.impl.operators.split.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;

/**
 * Adds new graph ids to the initial vertex set
 *
 * @param <V> EPGM vertex type
 */
@FunctionAnnotation.ReadFieldsFirst("graphIds")
@FunctionAnnotation.ReadFieldsSecond("f1")
public class AddNewGraphsToVertex<V extends Vertex>
  implements JoinFunction<V, Tuple2<GradoopId, GradoopIdSet>, V> {
  /**
   * {@inheritDoc}
   */
  @Override
  public V join(V vertex,
    Tuple2<GradoopId, GradoopIdSet> vertexWithGraphIds) {
    vertex.getGraphIds().addAll(vertexWithGraphIds.f1);
    return vertex;
  }

}
