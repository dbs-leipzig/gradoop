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

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.model.impl.operators.grouping.tuples.VertexGroupItem;

/**
 * Filter those tuples which are used to create new super vertices.
 */
@FunctionAnnotation.ReadFields("f5")
public class FilterSuperVertices implements FilterFunction<VertexGroupItem> {

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean filter(VertexGroupItem vertexGroupItem) throws Exception {
    return vertexGroupItem.isSuperVertex();
  }
}
