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

package org.gradoop.flink.representation.pojos;

import org.gradoop.flink.representation.transactional.adjacencylist.AdjacencyListCell;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Id-based comparator for adjacency list cells.
 * order: vertex id, edge id, direction
 *
 * @param <T> algorithm-specific cell value type.
 */
public class AdjacencyListCellComparator<T>
  implements Comparator<AdjacencyListCell<T>>, Serializable {

  @Override
  public int compare(AdjacencyListCell<T> a, AdjacencyListCell<T> b) {
    int comparison = a.getVertexId().compareTo(b.getVertexId());

    if (comparison == 0) {
      comparison = a.getEdgeId().compareTo(b.getEdgeId());

      if (comparison == 0 && a.isOutgoing() != b.isOutgoing()) {
        comparison = a.isOutgoing() ? -1 : 1;
      }
    }

    return comparison;
  }
}
