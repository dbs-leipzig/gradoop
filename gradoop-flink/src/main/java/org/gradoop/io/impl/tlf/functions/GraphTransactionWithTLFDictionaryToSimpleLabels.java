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

public class GraphTransactionWithTLFDictionaryToSimpleLabels<G extends
  EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge> implements
  MapFunction<GraphTransaction<G, V, E>, GraphTransaction<G, V, E>> {

  /**
   * True if the vertices of the transaction as labels set by a dictionary;
   */
  private boolean hasVertexDictionary;
  /**
   * True if the edges of the transaction as labels set by a dictionary;
   */
  private boolean hasEdgeDictionary;

  /**
   * Creates a new map function which sets the simple id from either the
   * vertex labels or the edge labels or both.
   * Changes labels from:
   *    'dictionarylabel;simpleId'
   * to:
   *    'simpleId'
   *
   * @param hasVertexDictionary set to true if vertex labels are set by a
   *                            dictionary
   * @param hasEdgeDictionary set to true if edge labels are set by a dictionary
   */
  public GraphTransactionWithTLFDictionaryToSimpleLabels(
    boolean hasVertexDictionary, boolean hasEdgeDictionary) {
    this.hasVertexDictionary = hasVertexDictionary;
    this.hasEdgeDictionary = hasEdgeDictionary;
  }

  /**
   * Removes the part of the label which contains the dictionary content and
   * keeps only the simple id.
   *
   * @param graphTransaction graph transaction with label format:
   *                         'dictionarylabel;simpleId'
   * @return graph transaction with label format: 'simpleId'
   * @throws Exception
   */
  @Override
  public GraphTransaction<G, V, E> map(
    GraphTransaction<G, V, E> graphTransaction) throws Exception {
    if (hasVertexDictionary) {
      for (V vertex : graphTransaction.getVertices()) {
        vertex.setLabel(vertex.getLabel().split(TLFDictionaryConstants
          .LABEL_SPLIT)[1]);
      }
    }
    if (hasEdgeDictionary) {
      for (E edge : graphTransaction.getEdges()) {
        edge.setLabel(edge.getLabel().split(TLFDictionaryConstants
          .LABEL_SPLIT)[1]);
      }
    }
    return graphTransaction;
  }
}
