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

package org.gradoop.flink.representation.common.adjacencylist;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Comparator for adjacency list cells.
 * order: edge data, vertex data
 *
 * @param <ED> edge data type.
 * @param <VD> vertex data type.
 */
public class AdjacencyListCellComparator<ED extends Comparable<ED>, VD extends Comparable<VD>>
  implements Comparator<AdjacencyListCell<ED, VD>>, Serializable {

  @Override
  public int compare(AdjacencyListCell<ED, VD> a, AdjacencyListCell<ED, VD> b) {
    int comparison = a.getVertexData().compareTo(b.getVertexData());

    if (comparison == 0) {
      comparison = a.getEdgeData().compareTo(b.getEdgeData());

      if (comparison == 0) {
        comparison = a.getVertexData().compareTo(b.getVertexData());
      }
    }

    return comparison;
  }
}
