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

package org.gradoop.flink.model.api.pojos;

import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 * A factory to create algorithm-specific value of adjacency list cells.
 * @param <T> algorithm-specific value type
 */
public interface AdjacencyListCellValueFactory<T> {
  /**
   * Returns algorithm-specific value for an edge triple
   *
   * @param source source vertex
   * @param edge edge
   * @param target target vertex
   * @return
   */
  T createValue(Vertex source, Edge edge, Vertex target);
}
