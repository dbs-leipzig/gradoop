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
