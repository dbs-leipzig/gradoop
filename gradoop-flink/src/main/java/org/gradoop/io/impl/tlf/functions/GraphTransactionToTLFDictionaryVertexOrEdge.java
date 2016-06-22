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

package org.gradoop.io.impl.tlf.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.tuples.GraphTransaction;

/**
 * Converts the label of each vertex into the format:
 * 'dictionaryLabel;simpleId'
 * where simpleId is an integer and starts at 0 and increments by 1 for each
 * vertex.
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class GraphTransactionToTLFDictionaryVertexOrEdge<G extends EPGMGraphHead, V
  extends EPGMVertex, E extends EPGMEdge> implements
  MapFunction<GraphTransaction<G, V, E>, GraphTransaction<G, V, E>>{

  /**
   * '1' if write a vertex dictionary, '-1' if write an edge dictionary.
   */
  private int dictionaryType;

  /**
   * Creates a new mapper which adds a new incrementing integer value at
   * the end of either the vertices or the edges.
   *
   * @param dictionaryType set to '1' for writing a vertex dictionary, and
   *                         '-1' for writing an edge dictionary
   */
  public GraphTransactionToTLFDictionaryVertexOrEdge(int dictionaryType) {
    this.dictionaryType = dictionaryType;
  }

  /**
   * Maps a new integer value plus ';' at the end of each vertex label
   *
   * @param graphTransaction transaction which contains one complete graph
   * @return graph transaction where each vertex has the new label
   * @throws Exception
   */
  @Override
  public GraphTransaction<G, V, E> map(
    GraphTransaction<G, V, E> graphTransaction) throws Exception {
    int id = 0;
    if (dictionaryType == TLFDictionaryConstants.VERTEX_DICTIONARY) {
      for (V vertex : graphTransaction.getVertices()) {
        vertex.setLabel(vertex.getLabel() +
          TLFDictionaryConstants.LABEL_SPLIT + id);
        id++;
      }
    } else if (dictionaryType == TLFDictionaryConstants.EDGE_DICTIONARY){
      for (E edge : graphTransaction.getEdges()) {
        edge.setLabel(edge.getLabel() +
          TLFDictionaryConstants.LABEL_SPLIT + id);
        id++;
      }
    }
    return graphTransaction;
  }
}
