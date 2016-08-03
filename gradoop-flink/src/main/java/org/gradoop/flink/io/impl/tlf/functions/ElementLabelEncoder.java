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

package org.gradoop.flink.io.impl.tlf.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.flink.io.impl.tlf.constants.BroadcastNames;
import org.gradoop.flink.model.impl.tuples.GraphTransaction;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;

import java.util.HashMap;
import java.util.Map;

/**
 * Map function which sets the simple label from either the vertex labels or
 * the edge labels or both.
 */
public class ElementLabelEncoder extends
  RichMapFunction<GraphTransaction, GraphTransaction> {

  /**
   * True if the vertices of the transaction as labels set by a dictionary.
   */
  private boolean hasVertexDictionary;
  /**
   * True if the edges of the transaction as labels set by a dictionary.
   */
  private boolean hasEdgeDictionary;
  /**
   * Map containing the vertex dictionary.
   */
  private Map<String, Integer> vertexDictionary;
  /**
   * Map containing the edge dictionary.
   */
  private Map<String, Integer> edgeDictionary;

  /**
   * Creates a new map function which sets the simple label from either the
   * vertex labels or the edge labels or both.
   * Changes labels from:
   *    'dictionarylabel'
   * to:
   *    'simpleLabel' (Integer)
   *
   * @param hasVertexDictionary set to true if vertex labels are set by a
   *                            dictionary
   * @param hasEdgeDictionary set to true if edge labels are set by a dictionary
   */
  public ElementLabelEncoder(
    boolean hasVertexDictionary, boolean hasEdgeDictionary) {
    this.hasVertexDictionary = hasVertexDictionary;
    this.hasEdgeDictionary = hasEdgeDictionary;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    //load the dictionaries if set
    if (hasVertexDictionary) {
      vertexDictionary = getRuntimeContext()
        .<HashMap<String, Integer>>getBroadcastVariable(
          BroadcastNames.VERTEX_DICTIONARY).get(0);
    }
    if (hasEdgeDictionary) {
      edgeDictionary = getRuntimeContext()
        .<HashMap<String, Integer>>getBroadcastVariable(
          BroadcastNames.EDGE_DICTIONARY).get(0);
    }
  }

  /**
   * Removes the dictionary labels and sets the simple labels (integer).
   *
   * @param graphTransaction graph transaction with label format:
   *                         'dictionarylabel'
   * @return graph transaction with label format: 'simpleLabel' (Integer)
   * @throws Exception
   */
  @Override
  public GraphTransaction map(GraphTransaction graphTransaction)
      throws Exception {
    if (vertexDictionary != null) {
      for (Vertex vertex : graphTransaction.getVertices()) {
        vertex.setLabel(vertexDictionary.get(vertex.getLabel()).toString());
      }
    }
    if (edgeDictionary != null) {
      for (Edge edge : graphTransaction.getEdges()) {
        edge.setLabel(edgeDictionary.get(edge.getLabel()).toString());
      }
    }
    return graphTransaction;
  }
}
