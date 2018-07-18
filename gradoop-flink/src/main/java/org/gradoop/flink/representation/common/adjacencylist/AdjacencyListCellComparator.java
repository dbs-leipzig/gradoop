/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
