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

package org.gradoop.flink.model.impl.operators.neighborhood;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.functions.EdgeAggregateFunction;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.SourceId;
import org.gradoop.flink.model.impl.functions.epgm.TargetId;
import org.gradoop.flink.model.impl.operators.neighborhood.functions.NeighborEdgeCoGroupFunction;

public class GroupReduceEdgesFunction extends EdgesFunction {

  public GroupReduceEdgesFunction(EdgeAggregateFunction function, EdgeDirection direction) {
    super(function, direction);
  }

  @Override
  public LogicalGraph execute(LogicalGraph graph) {
    DataSet<Vertex> vertices;
    switch (getDirection()) {
    case IN:
      vertices = graph.getEdges()
        .coGroup(graph.getVertices())
        .where(new TargetId<>()).equalTo(new Id<>())
        .with(new NeighborEdgeCoGroupFunction((EdgeAggregateFunction) getFunction()));
      break;
    case OUT:
      vertices = graph.getEdges()
        .coGroup(graph.getVertices())
        .where(new SourceId<>()).equalTo(new Id<>())
        .with(new NeighborEdgeCoGroupFunction((EdgeAggregateFunction) getFunction()));
      break;
    case BOTH:
      vertices = graph.getEdges()
        .coGroup(graph.getVertices())
        .where(new TargetId<>()).equalTo(new Id<>())
        .with(new NeighborEdgeCoGroupFunction((EdgeAggregateFunction) getFunction()))
        .union(graph.getEdges()
          .coGroup(graph.getVertices())
          .where(new SourceId<>()).equalTo(new Id<>())
          .with(new NeighborEdgeCoGroupFunction((EdgeAggregateFunction) getFunction())));
      break;
    default:
      vertices = null;
    }
    return LogicalGraph.fromDataSets(
      graph.getGraphHead(), vertices, graph.getEdges(), graph.getConfig());
  }

  @Override
  public String getName() {
    return GroupReduceEdgesFunction.class.getName();
  }
}
