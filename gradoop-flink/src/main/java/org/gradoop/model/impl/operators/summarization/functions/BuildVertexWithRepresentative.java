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
 * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.model.impl.operators.summarization.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.model.impl.operators.summarization.tuples.VertexGroupItem;
import org.gradoop.model.impl.operators.summarization.tuples.VertexWithRepresentative;

/**
 * Maps a {@link VertexGroupItem} to a {@link VertexWithRepresentative}.
 */
@FunctionAnnotation.ForwardedFields("f0;f1")
public class BuildVertexWithRepresentative
  implements MapFunction<VertexGroupItem, VertexWithRepresentative> {

  /**
   * Avoid object instantiation.
   */
  private final VertexWithRepresentative reuseTuple;

  /**
   * Creates mapper.
   */
  public BuildVertexWithRepresentative() {
    this.reuseTuple = new VertexWithRepresentative();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public VertexWithRepresentative map(VertexGroupItem vertexGroupItem) throws
    Exception {
    reuseTuple.setVertexId(vertexGroupItem.getVertexId());
    reuseTuple.setGroupRepresentativeVertexId(
      vertexGroupItem.getGroupRepresentative());
    return reuseTuple;
  }
}
