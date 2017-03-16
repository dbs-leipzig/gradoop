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

package org.gradoop.flink.model.impl.operators.grouping.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.operators.grouping.tuples.SuperVertexIdWithVertex;
import org.gradoop.flink.model.impl.operators.grouping.tuples.VertexWithSuperVertex;

/**
 * Creates a tuple which  all vertices by their id, which is stored in the super vertex group item
 */
public class BuildSuperVertexIdWithVertex
  implements JoinFunction<VertexWithSuperVertex, Vertex, SuperVertexIdWithVertex> {

  /**
   * Avoid object initialization in each call.
   */
  private SuperVertexIdWithVertex superVertexIdWithVertex;

  /**
   * Constructor to initialize object.
   */
  public BuildSuperVertexIdWithVertex() {
    superVertexIdWithVertex = new SuperVertexIdWithVertex();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public SuperVertexIdWithVertex join(VertexWithSuperVertex vertexWithSuperVertex, Vertex vertex)
    throws Exception {
    superVertexIdWithVertex.setSuperVertexid(vertexWithSuperVertex.getSuperVertexId());
    superVertexIdWithVertex.setVertex(vertex);
    return superVertexIdWithVertex;
  }
}