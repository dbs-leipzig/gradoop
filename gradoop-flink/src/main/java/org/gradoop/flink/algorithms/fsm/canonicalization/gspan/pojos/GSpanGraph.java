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

package org.gradoop.flink.algorithms.fsm.canonicalization.gspan.pojos;

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
  private Map<Integer, AdjacencyList> adjacencyLists;
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

    this.adjacencyLists = Maps
      .newHashMapWithExpectedSize(adjacencyLists.size());

    Integer vertexId = 0;
    for (AdjacencyList adjacencyList : adjacencyLists) {
      this.adjacencyLists.put(vertexId, adjacencyList);
      vertexId++;
    }

    this.subgraphEmbeddings = subgraphEmbeddings;
  }

  /**
   * Default constructor.
   */
  public GSpanGraph() {
    this.adjacencyLists = Maps.newHashMapWithExpectedSize(0);
    this.subgraphEmbeddings = Maps.newHashMapWithExpectedSize(0);
  }

  /**
   * Constructor from existing adjacency lists.
   * @param adjacencyLists adjacency lists
   */
  public GSpanGraph(Map<Integer, AdjacencyList> adjacencyLists) {
    this.adjacencyLists = adjacencyLists;
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

  public Map<Integer, AdjacencyList> getAdjacencyLists() {
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

    for (Map.Entry<Integer, AdjacencyList> adjacencyList :
      adjacencyLists.entrySet()) {

      builder
        .append("(").append(adjacencyList.getKey()).append(":")
        .append(adjacencyList.getValue().getFromVertexLabel()).append(")")
        .append(adjacencyList.getValue().getEntries()).append("\n");
    }

    return builder.toString();
  }
}
