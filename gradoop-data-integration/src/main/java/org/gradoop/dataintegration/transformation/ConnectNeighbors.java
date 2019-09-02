/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.dataintegration.transformation.functions.CreateCartesianNeighborhoodEdges;
import org.gradoop.dataintegration.transformation.impl.Neighborhood;
import org.gradoop.dataintegration.transformation.impl.NeighborhoodVertex;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.LabelIsIn;
import org.gradoop.flink.model.impl.functions.graphcontainment.AddToGraphBroadcast;

import java.util.List;
import java.util.Objects;

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
   * @param sourceVertexLabel       The label of the vertices the neighborhood is connected.
   * @param edgeDirection           The edge direction to consider.
   * @param neighborhoodVertexLabel The label of the neighboring vertices that should be connected.
   * @param newEdgeLabel            The label of the created edge between the neighbors.
   */
  public ConnectNeighbors(String sourceVertexLabel, Neighborhood.EdgeDirection edgeDirection,
    String neighborhoodVertexLabel, String newEdgeLabel) {
    this.sourceVertexLabel = Objects.requireNonNull(sourceVertexLabel);
    this.edgeDirection = Objects.requireNonNull(edgeDirection);
    this.neighborhoodVertexLabel = Objects.requireNonNull(neighborhoodVertexLabel);
    this.newEdgeLabel = Objects.requireNonNull(newEdgeLabel);
  }

  @Override
  public LogicalGraph execute(LogicalGraph graph) {

    // determine the vertices the neighborhood should be calculated for
    DataSet<EPGMVertex> verticesByLabel = graph.getVerticesByLabel(sourceVertexLabel);

    // prepare the graph
    LogicalGraph reducedGraph = graph
      .vertexInducedSubgraph(new LabelIsIn<>(sourceVertexLabel, neighborhoodVertexLabel));

    // determine the neighborhood by edge direction
    DataSet<Tuple2<EPGMVertex, List<NeighborhoodVertex>>> neighborhood =
      Neighborhood.getPerVertex(reducedGraph, verticesByLabel, edgeDirection);

    // calculate the new edges and add them to the original graph
    DataSet<EPGMEdge> newEdges = neighborhood.flatMap(
      new CreateCartesianNeighborhoodEdges<>(graph.getFactory().getEdgeFactory(), newEdgeLabel))
      .map(new AddToGraphBroadcast<>())
      .withBroadcastSet(graph.getGraphHead().map(new Id<>()), AddToGraphBroadcast.GRAPH_ID);

    return graph.getFactory()
      .fromDataSets(graph.getGraphHead(), graph.getVertices(), graph.getEdges().union(newEdges));
  }
}
