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
import com.google.common.collect.Maps;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Graph representation in Gradoop gSpan implementations.
 */
public class GSpanGraph implements Serializable {

  /**
   * EPGMVertex adjacency lists. Index is vertex identifier.
   */
  private List<AdjacencyList> adjacencyLists;
  /**
   * Embeddings (value) of supported DFS codes (key)
   */
  private Map<DFSCode, Collection<DFSEmbedding>> subgraphEmbeddings;

  /**
   * Constructor.
   *
   * @param adjacencyLists adjacency lists
   * @param subgraphEmbeddings initial DFS codes and embeddings
   */
  public GSpanGraph(
    List<AdjacencyList> adjacencyLists,
    Map<DFSCode, Collection<DFSEmbedding>> subgraphEmbeddings) {

    this.adjacencyLists = adjacencyLists;
    this.subgraphEmbeddings = subgraphEmbeddings;
  }

  /**
   * default constructor
   */
  public GSpanGraph() {
    this.adjacencyLists = Lists.newArrayListWithCapacity(0);
    this.subgraphEmbeddings = Maps.newHashMapWithExpectedSize(0);
  }

  /**
   * Convenience method to check further ability to grow frequent subgraphs.
   *
   * @return true, if able, false, otherwise
   */
  public Boolean hasGrownSubgraphs() {
    return this.subgraphEmbeddings != null;
  }

  public List<AdjacencyList> getAdjacencyLists() {
    return adjacencyLists;
  }

  public Map<DFSCode, Collection<DFSEmbedding>> getSubgraphEmbeddings() {
    return subgraphEmbeddings;
  }

  public void setSubgraphEmbeddings(
    Map<DFSCode, Collection<DFSEmbedding>> subgraphEmbeddings) {
    this.subgraphEmbeddings = subgraphEmbeddings;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();

    int listId = 0;

    for (AdjacencyList adjacencyList : adjacencyLists) {
      builder
        .append("(").append(listId).append(":")
        .append(adjacencyList.getFromVertexLabel()).append(")")
        .append(adjacencyList.getEntries()).append("\n");
      listId++;
    }

    return builder.toString();
  }
}
