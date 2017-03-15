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

package org.gradoop.examples.nestedmodel.datarepresentation.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.flink.representation.transactional.GraphTransaction;

import java.util.HashSet;
import java.util.Set;

/**
 * Mapping the GraphTransaction into a tuple (projection function)
 */
@FunctionAnnotation.ForwardedFieldsFirst("id -> f0")
public class VertexOrEdgeArray implements MapFunction<GraphTransaction, Tuple2<GradoopId, Set<GradoopId>>> {

  /**
   * Reusable element to be returned
   */
  private final Tuple2<GradoopId, Set<GradoopId>> reusable;

  /**
   * Checks which projection has to be applied
   */
  private final boolean isVertex;

  /**
   * Mappeing a GraphTransaction into GradoopIds
   * @param isVertex    Determines if the array is formed of the vertex ids or, else of the edges'
   */
  public VertexOrEdgeArray(boolean isVertex) {
    this.isVertex = isVertex;
    reusable = new Tuple2<>();
    reusable.f1 = new HashSet<>();
  }

  @Override
  public Tuple2<GradoopId, Set<GradoopId>> map(GraphTransaction value) throws Exception {
    reusable.f0 = value.f0.getId();
    reusable.f1.clear();
    (isVertex ? value.getVertices() : value.getEdges()).stream()
      .map(Element::getId)
      .forEach(reusable.f1::add);
    return reusable;
  }
}
