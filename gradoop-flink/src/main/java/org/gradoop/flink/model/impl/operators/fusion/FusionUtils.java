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

import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeInfoParser;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.SourceId;
import org.gradoop.flink.model.impl.functions.epgm.TargetId;
import org.gradoop.flink.model.impl.functions.graphcontainment.GraphContainmentFilterBroadcast;
import org.gradoop.flink.model.impl.functions.graphcontainment.InGraphBroadcast;
import org.gradoop.flink.model.impl.functions.graphcontainment.NotInGraphBroadcast;
import org.gradoop.flink.model.impl.functions.utils.RightSide;

/**
 * Created by Giacomo Bergami on 25/01/17.
 */
public class FusionUtils {

  /**
   * Recreate another graph which is a copy of the graph x provided as an input
   *
   * @param x Input graph
   * @return Copy graph
   */
  public static LogicalGraph recreateGraph(LogicalGraph x) {
    return LogicalGraph
      .fromDataSets(x.getGraphHead(), x.getVertices(), x.getEdges(), x.getConfig());
  }

  /**
   * Given g...
   *
   * @param g Graph where the id should be returned
   * @return returns its ID through a DataSet collection
   */
  public static DataSet<GradoopId> getGraphId(LogicalGraph g) {
    return g.getGraphHead().map(new Id<>());
  }

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
    return collection.filter(inGraph ? new InGraphBroadcast<>() : new NotInGraphBroadcast<>())
      .withBroadcastSet(getGraphId(g), GraphContainmentFilterBroadcast.GRAPH_ID);
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
    DataSet<Edge> filteredEdges =
      containment.getGraphHead().first(1).map((GraphHead x) -> x.getId())
        .cross(superGraph.getEdges())
        .with(new CrossFunction<GradoopId, Edge, Tuple2<GradoopId, Edge>>() {
          @Override
          public Tuple2<GradoopId, Edge> cross(GradoopId val1, Edge val2) throws Exception {
            return new Tuple2<>(val1, val2);
          }
        }).returns(TypeInfoParser.parse(
        "Tuple2<org.gradoop.common.model.impl.id" + ".GradoopId, org.gradoop.common.model" +
          ".impl.pojo.Edge>")).filter(new FilterFunction<Tuple2<GradoopId, Edge>>() {
            @Override
            public boolean filter(Tuple2<GradoopId, Edge> value) throws Exception {
              return value.f1.getGraphIds().contains(value.f0);
            }
          }).returns(TypeInfoParser.parse(
        "Tuple2<org.gradoop.common.model.impl.id" + ".GradoopId, org.gradoop.common.model" +
          ".impl.pojo.Edge>")).map((Tuple2<GradoopId, Edge> t) -> t.f1).returns(Edge.class);

    DataSet<Vertex> newVertices =
      filteredEdges.join(superGraph.getVertices()).where(new SourceId<>()).equalTo(new Id<Vertex>())
        .with(new RightSide<Edge, Vertex>()).union(
        filteredEdges.join(superGraph.getVertices()).where(new TargetId<>())
          .equalTo(new Id<Vertex>()).with(new RightSide<Edge, Vertex>()))
        .distinct(new Id<Vertex>());

    return LogicalGraph.fromDataSets(newVertices, filteredEdges, superGraph.getConfig());
  }

}
