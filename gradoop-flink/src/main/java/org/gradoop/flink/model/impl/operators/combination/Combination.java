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

package org.gradoop.flink.model.impl.operators.combination;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.operators.BinaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;

/**
 * Computes the combined graph from two logical graphs.
 */
public class Combination implements BinaryGraphToGraphOperator {

  /**
   * Creates a new logical graph by union the vertex and edge sets of two
   * input graphs. Vertex and edge equality is based on their respective
   * identifiers.
   *
   * @param firstGraph  first input graph
   * @param secondGraph second input graph
   * @return combined graph
   */
  @Override
  public LogicalGraph execute(LogicalGraph firstGraph,
    LogicalGraph secondGraph) {

    DataSet<Vertex> newVertexSet = firstGraph.getVertices()
      .union(secondGraph.getVertices())
      .distinct(new Id<Vertex>());

    DataSet<Edge> newEdgeSet = firstGraph.getEdges()
      .union(secondGraph.getEdges())
      .distinct(new Id<Edge>());

    return LogicalGraph.fromDataSets(
      newVertexSet, newEdgeSet, firstGraph.getConfig());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return Combination.class.getName();
  }

}
