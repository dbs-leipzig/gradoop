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
package org.gradoop.flink.model.impl.nested.datastructures.equality;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.nested.datastructures.NormalizedGraph;
import org.gradoop.flink.model.impl.operators.tostring.api.EdgeToString;
import org.gradoop.flink.model.impl.operators.tostring.api.GraphHeadToString;
import org.gradoop.flink.model.impl.operators.tostring.api.VertexToString;

/**
 * Operator to determine if two graph are equal according to given string
 * representations of graph heads, vertices and edges.
 */
public class NormalizedGraphEquality {

  /**
   * collection equality operator, wrapped by graph equality
   * (graph are considered to be a 1-graph collection)
   */
  private final NormalizedGraphEquality2 collectionEquality;
  /**
   * sets mode for directed or undirected graphs
   */
  private final boolean directed;

  /**
   * constructor to set string representations
   * @param graphHeadToString string representation of graph heads
   * @param vertexToString string representation of vertices
   * @param edgeToString string representation of edges
   * @param directed sets mode for directed or undirected graphs
   */
  public NormalizedGraphEquality(GraphHeadToString<GraphHead> graphHeadToString,
    VertexToString<Vertex> vertexToString, EdgeToString<Edge> edgeToString,
    boolean directed) {
    this.directed = directed;

    this.collectionEquality = new NormalizedGraphEquality2(
      graphHeadToString, vertexToString, edgeToString, this.directed);
  }

  public DataSet<Boolean> execute(
    NormalizedGraph firstGraph, NormalizedGraph secondGraph) {
    return collectionEquality.execute(firstGraph,secondGraph);
  }

  public String getName() {
    return this.getClass().getSimpleName();
  }
}
