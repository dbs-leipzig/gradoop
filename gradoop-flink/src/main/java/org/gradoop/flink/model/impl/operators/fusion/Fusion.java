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

import org.apache.flink.api.common.functions.BroadcastVariableInitializer;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.model.api.operators.BinaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.Serializable;

/**
 * Created by Giacomo Bergami on 19/01/17.
 */
public class Fusion implements BinaryGraphToGraphOperator {


  /**
   * Default configuration to be used if there are some problems with the input graphs
   */
  private static final GradoopFlinkConfig DEFAULT_CONF =
    GradoopFlinkConfig.createConfig(ExecutionEnvironment.getExecutionEnvironment());

  /**
   * @return The operator's name
   */
  @Override
  public String getName() {
    return Fusion.class.getName();
  }


  /**
   * Given a searchGraph and a patternGraph, returns the fused graph where pattern is replaced
   * inside the search by a single vertex having both label and properties belonging to the
   * pattern graph.
   *
   * @param searchGraph  Graph containing the actual data
   * @param patternGraph Graph containing the graph to be replaced within the search graph
   * @return A search graph containing vertices and edges logically belonging to the
   * search graph
   */
  @Override
  public LogicalGraph execute(final LogicalGraph searchGraph, final LogicalGraph patternGraph) {
    //Catching possible errors (null) and handling then as they were empty objects
    if (searchGraph == null) {
      return LogicalGraph
        .createEmptyGraph(patternGraph == null ? DEFAULT_CONF : patternGraph.getConfig());
    } else if (patternGraph == null) {
      return FusionUtils.recreateGraph(searchGraph);
    } else {
      DataSet<Vertex> leftVertices = searchGraph.getVertices();

            /*
             * Collecting the vertices that have to be removed and replaced by the aggregation's
             * result
             */
      DataSet<Vertex> toBeReplaced =
        FusionUtils.areElementsInGraph(leftVertices, patternGraph, true);

      // But, even the vertices that belong only to the search graph, should be added
      DataSet<Vertex> finalVertices =
        FusionUtils.areElementsInGraph(leftVertices, patternGraph, false);
      //////////////////////////////////////////

      final GradoopId vId = GradoopId.get();
      //final Properties prop = FusionUtils.getGraphProperties(patternGraph);
      //final String label = FusionUtils.getGraphLabel(patternGraph);

      // Then I create the graph that substitute the vertices within toBeReplaced
      DataSet<Vertex> toBeAdded;

      toBeAdded = searchGraph.getGraphHead().first(1).join(patternGraph.getGraphHead().first(1))
        .where((GraphHead g) -> 0).equalTo((GraphHead g) -> 0)
        .with(new FlatJoinFunction<GraphHead, GraphHead, Vertex>() {
          @Override
          public void join(GraphHead searchGraphHead, GraphHead patternGraphSeachHead,
            Collector<Vertex> out) throws Exception {
            Vertex v = new Vertex();
            v.setLabel(patternGraphSeachHead.getLabel());
            v.setProperties(patternGraphSeachHead.getProperties());
            v.setId(vId);
            v.addGraphId(searchGraphHead.getId());
            out.collect(v);
          }
        });

      /*
       * The newly created vertex v has to be created iff. we have some actual vertices to be
       * replaced, and then if toBeReplaced contains at least one element
       */
      DataSet<Vertex> addOnlyIfNecessary =
        toBeReplaced.first(1).join(toBeAdded).where((Vertex x) -> 0).equalTo((Vertex x) -> 0)
          .with(new JoinFunction<Vertex, Vertex, Vertex>() {
            @Override
            public Vertex join(Vertex first, Vertex second) throws Exception {
              // If there is at least one element to be replaced, then return the vertex that has
              // to be added. Otherwise, this function won't ever be called
              return second;
            }
          });

      DataSet<Vertex> toBeReturned = finalVertices.union(addOnlyIfNecessary);
      //////////////////////////////////////////

      //In the final graph, all the edges appearing only in the search graph should appear
      DataSet<Edge> leftEdges = searchGraph.getEdges();
      leftEdges = FusionUtils.areElementsInGraph(leftEdges, patternGraph, false);

            /*
             * Concerning the other edges, we have to eventually update them and to be linked
             * with the new vertex
             * The following expression could be formalized as follows:
             *
             * updatedEdges = map(E, x ->
             *      e' <- onNextUpdateof(x).newfrom(x);
             *      if (e'.src \in toBeReplaced) e'.src = vId
             *      end if
             *      if (e'.dst \in toBeReplaced) e'.dst = vId
             *      end if
             *      return e'
             * )
             *
             */
      DataSet<Edge> updatedEdges =
        leftEdges.fullOuterJoin(toBeReplaced).where((Edge x) -> x.getSourceId())
          .equalTo((Vertex y) -> y.getId())
          .with((FlatJoinFunction<Edge, Vertex, Edge>) (edge, vertex, collector) -> {
            if (vertex == null) {
              collector.collect(edge);
            } else if (edge != null) {
              Edge e = new Edge();
              e.setId(GradoopId.get());
              e.setSourceId(vId);
              e.setTargetId(edge.getTargetId());
              e.setProperties(edge.getProperties());
              e.setLabel(edge.getLabel());
              e.setGraphIds(edge.getGraphIds());
              collector.collect(e);
            }
          }).returns(Edge.class).fullOuterJoin(toBeReplaced).where((Edge x) -> x.getTargetId())
          .equalTo((Vertex y) -> y.getId())
          .with((FlatJoinFunction<Edge, Vertex, Edge>) (edge, vertex, collector) -> {
            if (vertex == null) {
              collector.collect(edge);
            } else if (edge != null) {
              Edge e = new Edge();
              e.setId(GradoopId.get());
              e.setTargetId(vId);
              e.setSourceId(edge.getSourceId());
              e.setProperties(edge.getProperties());
              e.setLabel(edge.getLabel());
              e.setGraphIds(edge.getGraphIds());
              collector.collect(e);
            }
          }).returns(Edge.class);

      // All's well what ends well… farewell!
      return LogicalGraph.fromDataSets(searchGraph.getGraphHead(), toBeReturned, updatedEdges,
        searchGraph.getConfig());


    }
  }

}
