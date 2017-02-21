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

package org.gradoop.flink.model.impl.operators.fusion;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.SourceId;
import org.gradoop.flink.model.impl.functions.epgm.TargetId;
import org.gradoop.flink.model.impl.functions.graphcontainment.GraphContainmentFilterBroadcast;
import org.gradoop.flink.model.impl.functions.graphcontainment.InGraphBroadcast;
import org.gradoop.flink.model.impl.functions.graphcontainment.NotInGraphBroadcast;
import org.gradoop.flink.model.impl.functions.tuple.Value1Of2;
import org.gradoop.flink.model.impl.functions.utils.RightSide;
import org.gradoop.flink.model.impl.operators.fusion.functions.FilterSubgraphEdges;

/**
 *
 * Contains some utility functions for the fusion operator
 *
 * Created by Giacomo Bergami on 25/01/17.
 */
public class VertexFusionUtils {
  /**
   * Filters a collection form a graph dataset performing either an intersection or a difference
   *
   * @param collection Collection to be filtered
   * @param g          Graph where to verify the containment operation
   * @param inGraph    If the value is true, then perform an intersection, otherwise a difference
   * @param <P>        e.g. either vertices or edges
   * @return The filtered collection
   */
  public static <P extends GraphElement> DataSet<P> areElementsInGraph(DataSet<P> collection,
    LogicalGraph g, boolean inGraph) {
    return collection
      .filter(inGraph ? new InGraphBroadcast<>() : new NotInGraphBroadcast<>())
      .withBroadcastSet(g.getGraphHead().map(new Id<>()), GraphContainmentFilterBroadcast.GRAPH_ID);
  }

  /**
   * uses the definition of edgeInducedSubgraph for a specific task
   *
   * @param containment Logical Graph used for the containment operation
   * @param superGraph  Super graph from which extract the subgraph
   * @return the subgraph of superGraph
   */
  public static LogicalGraph myInducedEdgeSubgraphForFusion(LogicalGraph containment,
    LogicalGraph superGraph) {

    //return the edges from the superGraph that are contained
    DataSet<Edge> filteredEdges = containment.getGraphHead()
      .first(1)
      .map(new Id<>())
      .cross(superGraph.getEdges())
      .filter(new FilterSubgraphEdges())
      .map(new Value1Of2<>());

    DataSet<Vertex> tobeUnitedWith = filteredEdges
      .join(superGraph.getVertices())
      .where(new TargetId<>()).equalTo(new Id<>())
      .with(new RightSide<>());

    DataSet<Vertex> newVertices = filteredEdges
      .join(superGraph.getVertices())
      .where(new SourceId<>()).equalTo(new Id<>())
      .with(new RightSide<>())
      .union(tobeUnitedWith)
      .distinct(new Id<>());

    return LogicalGraph.fromDataSets(newVertices, filteredEdges, superGraph.getConfig());
  }

}
