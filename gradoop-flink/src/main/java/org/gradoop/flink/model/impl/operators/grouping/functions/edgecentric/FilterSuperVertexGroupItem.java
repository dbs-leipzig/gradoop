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

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.flink.model.impl.operators.grouping.tuples.edgecentric.SuperVertexGroupItem;

/**
 * Filter to get items which either represent new, concatenated super vertices or those which
 * represent vertices which stay unchanged.
 */
public class FilterSuperVertexGroupItem implements FilterFunction<SuperVertexGroupItem> {

  /**
   * True, to get concatenated super vertex group items, false to get items where the vertex
   * stays unchanged.
   */
  private boolean filterConcatenatedSuperVertices;

  /**
   * Constructor.
   *
   * @param filterConcatenatedSuperVertices true, for concatenated items, false for unchanged
   *                                        vertices
   */
  public FilterSuperVertexGroupItem(boolean filterConcatenatedSuperVertices) {
    this.filterConcatenatedSuperVertices = filterConcatenatedSuperVertices;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean filter(SuperVertexGroupItem superVertexGroupItem) throws Exception {
    if (filterConcatenatedSuperVertices) {
      return superVertexGroupItem.getVertexIds().size() > 1;
    } else {
      // the only element in the set of super vertex ids is the vertex id itself
      return (superVertexGroupItem.getVertexIds().size() == 1) &&
        (superVertexGroupItem.getVertexIds().iterator().next().equals(
          superVertexGroupItem.getSuperVertexId()));
    }
  }
}
