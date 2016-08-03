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

package org.gradoop.flink.algorithms.fsm.gspan.pojos;

import com.google.common.collect.Lists;

import java.io.Serializable;
import java.util.Collection;

/**
 * pojo representing an adjacency list
 */
public class AdjacencyList implements Serializable {
  /**
   * label of the associated vertex
   */
  private final Integer fromVertexLabel;
  /**
   * adjacency list entries
   */
  private final Collection<AdjacencyListEntry> entries;

  /**
   * constructor
   * @param fromVertexLabel vertex label
   */
  public AdjacencyList(Integer fromVertexLabel) {
    this.fromVertexLabel = fromVertexLabel;
    this.entries = Lists.newArrayList();
  }

  @Override
  public String toString() {
    return entries.toString();
  }

  public Integer getFromVertexLabel() {
    return fromVertexLabel;
  }

  public Collection<AdjacencyListEntry> getEntries() {
    return entries;
  }
}
