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
package org.gradoop.flink.io.impl.tlf.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.flink.io.impl.tlf.TLFConstants;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;
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
          TLFConstants.VERTEX_DICTIONARY).get(0);
    }
    if (hasEdgeDictionary) {
      edgeDictionary = getRuntimeContext()
        .<HashMap<String, Integer>>getBroadcastVariable(
          TLFConstants.EDGE_DICTIONARY).get(0);
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
