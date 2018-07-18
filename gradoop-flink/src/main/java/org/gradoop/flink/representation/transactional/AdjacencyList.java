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
package org.gradoop.flink.representation.transactional;

import org.apache.flink.api.java.tuple.Tuple5;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.representation.common.adjacencylist.AdjacencyListRow;

import java.util.Map;

/**
 * Traversal optimized representation of a graph transaction.
 *
 * f0 : graph head
 * f1 : vertex/edge id -> label
 * f2 : vertex/edge id -> properties
 * f3 : vertex id -> outgoing adjacency rows
 * f4 : vertex id -> incoming adjacency rows
 *
 * @param <ID> ID type
 * @param <L> label type
 * @param <ED> edge data type
 * @param <VD> vertex data type
 */
public class AdjacencyList<ID extends Comparable<ID>, L extends Comparable<L>, ED, VD> extends
  Tuple5<
    GraphHead,
    Map<ID, L>,
    Map<ID, Properties>,
    Map<ID, AdjacencyListRow<ED, VD>>,
    Map<ID, AdjacencyListRow<ED, VD>>
  > {

  /**
   * Default constructor.
   */
  public AdjacencyList() {
  }

  /**
   * Constructor.
   * @param graphHead graph id
   * @param labels graph, vertex and edge labels
   * @param properties graph, vertex and edge properties
   * @param outgoingRows outgoing adjacency list rows
   * @param incomingRows incoming adjacency list rows
   */
  public AdjacencyList(GraphHead graphHead, Map<ID, L> labels, Map<ID, Properties> properties,
    Map<ID, AdjacencyListRow<ED, VD>> outgoingRows, Map<ID, AdjacencyListRow<ED, VD>> incomingRows)
  {
    super(graphHead, labels, properties, outgoingRows, incomingRows);
  }

  /**
   * Label accessor.
   *
   * @param elementId graph / vertex / edge id
   *
   * @return label
   */
  public L getLabel(ID elementId) {
    return f1.get(elementId);
  }

  /**
   * Property accessor.
   * @param elementId graph / vertex / edge id
   *
   * @return property list
   */
  public Properties getProperties(ID elementId) {
    return f2.get(elementId);
  }

  public GraphHead getGraphHead() {
    return f0;
  }

  public void setGraphHead(GraphHead graphId) {
    this.f0 = graphId;
  }

  public Map<ID, AdjacencyListRow<ED, VD>> getOutgoingRows() {
    return f3;
  }

  public Map<ID, AdjacencyListRow<ED, VD>> getIncomingRows() {
    return f4;
  }
}
