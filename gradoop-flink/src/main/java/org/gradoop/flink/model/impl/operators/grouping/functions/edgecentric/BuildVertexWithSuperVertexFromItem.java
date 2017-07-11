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

package org.gradoop.flink.model.impl.operators.grouping.functions.edgecentric;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.grouping.tuples.edgecentric.SuperVertexGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.tuples.VertexWithSuperVertex;

/**
 * Maps the related ids for each vertex which is part of a super vertex .
 */
@FunctionAnnotation.ForwardedFields("f1")
@FunctionAnnotation.ReadFields("f0")
public class BuildVertexWithSuperVertexFromItem
  implements FlatMapFunction<SuperVertexGroupItem, VertexWithSuperVertex> {

  /**
   * Avoid object initialization in each call.
   */
  private VertexWithSuperVertex vertexWithSuperVertex = new VertexWithSuperVertex();

  /**
   * {@inheritDoc}
   */
  @Override
  public void flatMap(SuperVertexGroupItem superVertexGroupItem,
    Collector<VertexWithSuperVertex> collector) throws Exception {

    vertexWithSuperVertex.setSuperVertexId(superVertexGroupItem.getSuperVertexId());
    for (GradoopId gradoopId : superVertexGroupItem.getVertexIds()) {
      vertexWithSuperVertex.setVertexId(gradoopId);
      collector.collect(vertexWithSuperVertex);
    }
  }
}
