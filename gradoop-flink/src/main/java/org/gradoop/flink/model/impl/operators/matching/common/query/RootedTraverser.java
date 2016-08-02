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

package org.gradoop.flink.model.impl.operators.matching.common.query;

/**
 * A {@link Traverser} that can start at a specified query vertex.
 */
public interface RootedTraverser extends Traverser {

  /**
   * Traverse the query graph starting at the given vertex.
   *
   * @param rootVertex start vertex
   * @return traversal code
   */
  TraversalCode traverse(long rootVertex);
}
