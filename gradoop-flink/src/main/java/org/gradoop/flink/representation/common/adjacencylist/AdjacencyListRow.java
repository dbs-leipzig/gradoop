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

import com.google.common.collect.Lists;

import java.io.Serializable;
import java.util.Collection;

/**
 * Traversal optimized representation of a vertex.
 * @param <ED> type of algorithm specific cell value
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
