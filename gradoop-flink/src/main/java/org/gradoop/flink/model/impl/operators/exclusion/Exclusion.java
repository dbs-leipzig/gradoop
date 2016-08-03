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

/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or transform
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
 * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.model.impl.operators.exclusion;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.TargetId;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.flink.model.api.operators.BinaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.SourceId;
import org.gradoop.flink.model.impl.functions.graphcontainment.NotInGraphBroadcast;
import org.gradoop.flink.model.impl.functions.utils.LeftSide;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Computes the exclusion graph from two logical graphs.
 */
public class Exclusion implements BinaryGraphToGraphOperator {

  /**
   * Creates a new logical graph containing only vertices and edges that exist
   * in the first input graph but not in the second input graph. Vertex and edge
   * equality is based on their respective identifiers.
   *
   * @param firstGraph  first input graph
   * @param secondGraph second input graph
   * @return first graph without elements from second graph
   */
  @Override
  public LogicalGraph execute(
    LogicalGraph firstGraph, LogicalGraph secondGraph) {

    DataSet<GradoopId> graphId = secondGraph.getGraphHead()
      .map(new Id<GraphHead>());

    DataSet<Vertex> newVertexSet = firstGraph.getVertices()
      .filter(new NotInGraphBroadcast<Vertex>())
      .withBroadcastSet(graphId, NotInGraphBroadcast.GRAPH_ID);

    DataSet<Edge> newEdgeSet = firstGraph.getEdges()
      .filter(new NotInGraphBroadcast<Edge>())
      .withBroadcastSet(graphId, NotInGraphBroadcast.GRAPH_ID)
      .join(newVertexSet)
      .where(new SourceId<>())
      .equalTo(new Id<Vertex>())
      .with(new LeftSide<Edge, Vertex>())
      .join(newVertexSet)
      .where(new TargetId<>())
      .equalTo(new Id<Vertex>())
      .with(new LeftSide<Edge, Vertex>());

    return LogicalGraph.fromDataSets(
      newVertexSet, newEdgeSet, firstGraph.getConfig());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return Exclusion.class.getName();
  }
}
