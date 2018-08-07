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

import com.google.common.collect.Lists;

import java.io.Serializable;
import java.util.Collection;

/**
 * Traversal optimized representation of a vertex.
 *
 * @param <ED> edge data type
 * @param <VD> vertex data type
 */
public class AdjacencyListRow<ED, VD> implements Serializable {

  /**
   * collection of adjacency list cells
   */
  private Collection<AdjacencyListCell<ED, VD>> cells;

  /**
   * Default constructor.
   */
  public AdjacencyListRow() {
    this.cells = Lists.newArrayList();
  }

  /**
   * Constructor.
   *
   * @param cells collection of adjacency list cells
   */
  public AdjacencyListRow(Collection<AdjacencyListCell<ED, VD>> cells) {
    this.cells = cells;
  }

  @Override
  public String toString() {
    return cells.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    AdjacencyListRow that = (AdjacencyListRow) o;

    return cells.equals(that.cells);
  }

  @Override
  public int hashCode() {
    return cells.hashCode();
  }

  public Collection<AdjacencyListCell<ED, VD>> getCells() {
    return cells;
  }

  public void setCells(Collection<AdjacencyListCell<ED, VD>> cells) {
    this.cells = cells;
  }
}
