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
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.dataintegration.transformation.functions.CreateEdgesFromTriple;
import org.gradoop.dataintegration.transformation.functions.CreateVertexFromEdges;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.graphcontainment.AddToGraphBroadcast;
import org.gradoop.flink.model.impl.functions.tuple.Value0Of3;

import java.util.Objects;

/**
 * For edges of a specific label this graph transformation creates a new vertex containing the
 * properties of the edge and two new edges respecting the direction of the original edge.
 * The newly created edges and vertex labels are user-defined.
 * <p>
 * The original edges are still part of the resulting graph.
 * Use a {@link org.apache.flink.api.common.functions.FilterFunction} on the original label to
 * remove them.
 */
public class EdgeToVertex implements UnaryGraphToGraphOperator {

  /**
   * The label of the edges use for the transformation.
   */
  private final String edgeLabel;

  /**
   * The label of the newly created vertex.
   */
  private final String newVertexLabel;

  /**
   * The label of the newly created edge which points to the newly created vertex.
   */
  private final String edgeLabelSourceToNew;

  /**
   * The label of the newly created edge which starts at the newly created vertex.
   */
  private final String edgeLabelNewToTarget;

  /**
   * The constructor for the structural transformation.
   *
   * @param edgeLabel            The label of the edges use for the transformation.
   *                             (No edges will be transformed if this parameter is {@code null}).
   * @param newVertexLabel       The label of the newly created vertex.
   * @param edgeLabelSourceToNew The label of the newly created edge which points to the newly
   *                             created vertex.
   * @param edgeLabelNewToTarget The label of the newly created edge which starts at the newly
   *                             created vertex.
   */
  public EdgeToVertex(String edgeLabel, String newVertexLabel, String edgeLabelSourceToNew,
    String edgeLabelNewToTarget) {
    this.edgeLabel = edgeLabel;
    this.newVertexLabel = Objects.requireNonNull(newVertexLabel);
    this.edgeLabelSourceToNew = Objects.requireNonNull(edgeLabelSourceToNew);
    this.edgeLabelNewToTarget = Objects.requireNonNull(edgeLabelNewToTarget);
  }

  @Override
  public LogicalGraph execute(LogicalGraph graph) {
    DataSet<Edge> relevantEdges = graph.getEdgesByLabel(edgeLabel);

    // create new vertices
    DataSet<Tuple3<Vertex, GradoopId, GradoopId>> newVerticesAndOriginIds = relevantEdges
      .map(new CreateVertexFromEdges<>(newVertexLabel, graph.getFactory().getVertexFactory()));

    DataSet<Vertex> newVertices = newVerticesAndOriginIds
      .map(new Value0Of3<>())
      .map(new AddToGraphBroadcast<>())
      .withBroadcastSet(graph.getGraphHead().map(new Id<>()), AddToGraphBroadcast.GRAPH_ID)
      .union(graph.getVertices());

    // create edges to the newly created vertex
    DataSet<Edge> newEdges = newVerticesAndOriginIds
      .flatMap(new CreateEdgesFromTriple<>(graph.getFactory().getEdgeFactory(),
        edgeLabelSourceToNew, edgeLabelNewToTarget))
      .map(new AddToGraphBroadcast<>())
      .withBroadcastSet(graph.getGraphHead().map(new Id<>()), AddToGraphBroadcast.GRAPH_ID)
      .union(graph.getEdges());

    return graph.getFactory().fromDataSets(graph.getGraphHead(), newVertices, newEdges);
  }

  @Override
  public String getName() {
    return EdgeToVertex.class.getName();
  }
}
