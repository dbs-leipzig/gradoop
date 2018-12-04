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

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.dataintegration.transformation.functions.EdgesFromLocalTransitiveClosure;
import org.gradoop.dataintegration.transformation.functions.Neighborhood;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.neighborhood.keyselector.IdInTuple;

import java.util.List;


/**
 * For a given vertex label this graph transformation takes all neighbours per vertex with this
 * label and calculates the transitive closure for this subgraph. An edge is created between all
 * vertice pairs that fulfill the transitive closure requirement.
 * Furthermore each of those created edges contains the labels of the edges to create the
 * transitive closure and the former properties of the vertex. <br><br>
 * <p>
 * Each edge that has to be created results from a path in the graph of the following form: <br>
 * v_i --e_i,j--> v_j --e_j,k--> v_k <br>
 * The newly created edge goes from: v_i --e_i,k--> v_k <br>
 * The edge e_i,k has a user-defined label and besides the original vertex properties three
 * additional properties: <br>
 * - originalVertexLabel <br>
 * - firstEdgeLabel = labelOf(e_i,j) <br>
 * - secondEdgeLabel = labelOf(e_j,k)
 */
public class VertexToEdge implements UnaryGraphToGraphOperator {

  /**
   * The vertex label of v_j.
   */
  private final String centralVertexLabel;

  /**
   * The edge label for new edges.
   */
  private final String newEdgeLabel;

  /**
   * The constructor of the operator to transform vertices into edges.
   *
   * @param centralVertexLabel The vertex label of v_j.
   * @param newEdgeLabel The edge label for new edges.
   */
  public VertexToEdge(String centralVertexLabel, String newEdgeLabel) {

    this.centralVertexLabel = centralVertexLabel;
    this.newEdgeLabel = newEdgeLabel;
  }

  @Override
  public LogicalGraph execute(LogicalGraph graph) {

    DataSet<Tuple2<Vertex, List<Neighborhood.VertexPojo>>> incomingNeighborhood = Neighborhood
        .getPerVertex(graph,
          graph.getVerticesByLabel(centralVertexLabel),
          Neighborhood.EdgeDirection.INCOMING);

    DataSet<Tuple2<Vertex, List<Neighborhood.VertexPojo>>> outgoingNeighborhood = Neighborhood
        .getPerVertex(graph,
          graph.getVerticesByLabel(centralVertexLabel),
          Neighborhood.EdgeDirection.OUTGOING);

    if (incomingNeighborhood != null && outgoingNeighborhood != null) {
      DataSet<Edge> newEdges = incomingNeighborhood
          .coGroup(outgoingNeighborhood)
          .where(new IdInTuple<>(0))
          .equalTo(new IdInTuple<>(0))
          .with(new EdgesFromLocalTransitiveClosure(newEdgeLabel,
            graph.getConfig().getEdgeFactory()));

      return graph.getConfig().getLogicalGraphFactory()
          .fromDataSets(graph.getVertices(),
            graph.getEdges().union(newEdges));
    }
    return null;
  }

  @Override
  public String getName() {
    return this.getClass().getName();
  }
}
