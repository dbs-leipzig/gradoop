
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
