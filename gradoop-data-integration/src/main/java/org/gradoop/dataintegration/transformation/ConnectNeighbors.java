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
package org.gradoop.dataintegration.transformation;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.dataintegration.transformation.functions.Neighborhood;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.subgraph.functions.LabelIsIn;

import java.util.List;

/**
 * This graph transformation adds new edges to the graph. Those edges are created if a vertex of a
 * user-defined label has two neighbors of another user-defined label. A bidirectional (two edges
 * in gradoop) edge is then created between those two neighbors.
 */
public class ConnectNeighbors implements UnaryGraphToGraphOperator {

  /**
   * The label of the vertices the neighborhood is connected.
   */
  private final String sourceVertexLabel;

  /**
   * The edge direction to consider.
   */
  private final Neighborhood.EdgeDirection edgeDirection;

  /**
   * The label of the neighboring vertices that should be connected.
   */
  private final String neighborhoodVertexLabel;

  /**
   * The label of the created edge between the neighbors.
   */
  private final String newEdgeLabel;

  /**
   * The constructor to connect the neighbors of vertices with a certain label.
   *
   * @param sourceVertexLabel The label of the vertices the neighborhood is connected.
   * @param edgeDirection The edge direction to consider.
   * @param neighborhoodVertexLabel The label of the neighboring vertices that should be connected.
   * @param newEdgeLabel The label of the created edge between the neighbors.
   */
  public ConnectNeighbors(String sourceVertexLabel, Neighborhood.EdgeDirection edgeDirection,
                          String neighborhoodVertexLabel, String newEdgeLabel) {
    this.sourceVertexLabel = sourceVertexLabel;
    this.edgeDirection = edgeDirection;
    this.neighborhoodVertexLabel = neighborhoodVertexLabel;
    this.newEdgeLabel = newEdgeLabel;
  }


  @Override
  public LogicalGraph execute(LogicalGraph graph) {

    // determine the vertices the neighborhood should be calculated for
    DataSet<Vertex> verticesByLabel = graph.getVerticesByLabel(sourceVertexLabel);

    // prepare the graph
    LogicalGraph reducedGraph = graph
        .vertexInducedSubgraph(new LabelIsIn<>(sourceVertexLabel, neighborhoodVertexLabel));

    // determine the neighborhood by edge direction
    DataSet<Tuple2<Vertex, List<Neighborhood.VertexPojo>>> neighborhood =
        Neighborhood.getPerVertex(reducedGraph, verticesByLabel, edgeDirection);

    // calculate the new edges and add them to the original graph
    if (neighborhood != null) {
      DataSet<Edge> newEdges = neighborhood.flatMap(
        new CreateCartesianNeighborhoodEdges(graph.getConfig().getEdgeFactory(), newEdgeLabel));

      return graph.getConfig().getLogicalGraphFactory()
          .fromDataSets(
              graph.getVertices(),
              graph.getEdges().union(newEdges));
    }
    return null;
  }

  @Override
  public String getName() {
    return this.getClass().getName();
  }

  /**
   * The {@link FlatMapFunction} creates all edges between neighbor vertices.
   */
  private static class CreateCartesianNeighborhoodEdges implements FlatMapFunction<Tuple2<Vertex,
    List<Neighborhood.VertexPojo>>, Edge> {

    /**
     * The label of the created edge between the neighbors.
     */
    private final String newEdgeLabel;

    /**
     * The factory the edges are created with.
     */
    private final EdgeFactory factory;

    /**
     * The constructor to calculate the edges in the neighborhood.
     *
     * @param factory The factory the edges are created with.
     * @param newEdgeLabel The label of the created edge between the neighbors.
     */
    CreateCartesianNeighborhoodEdges(EdgeFactory factory, String newEdgeLabel) {
      this.newEdgeLabel = newEdgeLabel;
      this.factory = factory;
    }

    @Override
    public void flatMap(Tuple2<Vertex, List<Neighborhood.VertexPojo>> value, Collector<Edge> out) {
      List<Neighborhood.VertexPojo> neighbors = value.f1;

      // To "simulate" bidirectional edges we have to create an edge for each direction.
      for (int i = 0; i < neighbors.size(); i++) {
        for (int j = 0; j < neighbors.size(); j++) {
          if (i != j) {
            out.collect(factory.createEdge(newEdgeLabel, neighbors.get(i).getNeighborId(),
              neighbors.get(j).getNeighborId()));
          }
        }
      }
    }
  }
}
