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

package org.gradoop.model.impl.operators.grouping.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.model.impl.operators.grouping.tuples.VertexWithSuperVertex;
import org.gradoop.model.impl.operators.grouping.tuples.VertexGroupItem;

/**
 * Maps a {@link VertexGroupItem} to a {@link VertexWithSuperVertex}.
 */
@FunctionAnnotation.ForwardedFields(
  "f0;" + // vertex id
  "f1"    // super vertex id
)
public class BuildVertexWithSuperVertex
  implements MapFunction<VertexGroupItem, VertexWithSuperVertex> {

  /**
   * Avoid object instantiation.
   */
  private final VertexWithSuperVertex reuseTuple;

  /**
   * Creates mapper.
   */
  public BuildVertexWithSuperVertex() {
    this.reuseTuple = new VertexWithSuperVertex();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public VertexWithSuperVertex map(VertexGroupItem vertexGroupItem) throws
    Exception {
    reuseTuple.setVertexId(vertexGroupItem.getVertexId());
    reuseTuple.setSuperVertexId(vertexGroupItem.getSuperVertexId());
    return reuseTuple;
  }
}
