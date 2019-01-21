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
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.dataintegration.transformation.functions.EdgesFromLocalTransitiveClosure;
import org.gradoop.dataintegration.transformation.impl.Neighborhood;
import org.gradoop.dataintegration.transformation.impl.NeighborhoodVertex;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.neighborhood.keyselector.IdInTuple;

import java.util.List;
import java.util.Objects;

/**
 * For a given vertex label this graph transformation takes all neighbors per vertex with this
 * label and calculates the transitive closure for this subgraph. An edge is created between all
 * vertex pairs that fulfill the transitive closure requirement.
 * Furthermore each of those created edges contains the labels of the edges to create the
 * transitive closure and the former properties of the vertex.
 * <p>
 * Each edge that has to be created results from a path in the graph of the following form: <br>
 * {@code (i)-[i_j]->(j)-[j_k]->(k)} <br>
 * The newly created edge goes from: {@code (i)-[e_ik]->(k)} <br>
 * The edge {@code [e_ik]} has a user-defined label and besides the original vertex properties
 * three additional properties:
 * <ul>
 *   <li>{@code originalVertexLabel}</li>
 *   <li>{@code firstEdgeLabel = labelOf(i_j)}</li>
 *   <li>{@code secondEdgeLabel = labelOf(j_k)}</li>
 * </ul>
 */
public class VertexToEdge implements UnaryGraphToGraphOperator {

  /**
   * The vertex label of {@code j}.
   */
  private final String centralVertexLabel;

  /**
   * The edge label for new edges.
   */
  private final String newEdgeLabel;

  /**
   * The constructor of the operator to transform vertices into edges.
   *
   * @param centralVertexLabel The vertex label of {@code j}.
   * @param newEdgeLabel The edge label for new edges.
   */
  public VertexToEdge(String centralVertexLabel, String newEdgeLabel) {
    this.centralVertexLabel = Objects.requireNonNull(centralVertexLabel);
    this.newEdgeLabel = Objects.requireNonNull(newEdgeLabel);
  }

  @Override
  public LogicalGraph execute(LogicalGraph graph) {
    DataSet<Tuple2<Vertex, List<NeighborhoodVertex>>> incomingNeighborhood = Neighborhood
      .getPerVertex(graph, graph.getVerticesByLabel(centralVertexLabel),
        Neighborhood.EdgeDirection.INCOMING);

    DataSet<Tuple2<Vertex, List<NeighborhoodVertex>>> outgoingNeighborhood = Neighborhood
      .getPerVertex(graph, graph.getVerticesByLabel(centralVertexLabel),
        Neighborhood.EdgeDirection.OUTGOING);

    DataSet<Edge> newEdges = incomingNeighborhood
      .coGroup(outgoingNeighborhood)
      .where(new IdInTuple<>(0))
      .equalTo(new IdInTuple<>(0))
      .with(new EdgesFromLocalTransitiveClosure<>(newEdgeLabel,
        graph.getFactory().getEdgeFactory()));

    return graph.getFactory().fromDataSets(graph.getVertices(), graph.getEdges().union(newEdges));
  }

  @Override
  public String getName() {
    return VertexToEdge.class.getName();
  }
}
