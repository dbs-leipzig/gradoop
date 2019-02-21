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
package org.gradoop.dataintegration.transformation.impl;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.dataintegration.transformation.functions.CreateNeighborList;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.SourceId;
import org.gradoop.flink.model.impl.functions.epgm.TargetId;

import java.util.List;
import java.util.Objects;

/**
 * This class contains everything related to the neighborhood of vertices.
 * A vertex pojo, edge direction and a method to calculate the neighborhood of all vertices.
 */
public class Neighborhood {

  /**
   * This methods returns a {@link DataSet} containing a tuple where the first part is a vertex
   * and the second one a list of all neighbors of this vertex. The vertices are derived from the
   * centralVertices DataSet while the List elements are taken from the graph.
   *
   * @param graph A Graph the operation is executed on.
   * @param centralVertices The vertices the neighborhood should be calculated for.
   * @param edgeDirection The relevant direction for neighbors.
   * @return A Dataset of tuples containing vertices and their neighborhood.
   * @throws NullPointerException if any of the parameters is null.
   */
  public static DataSet<Tuple2<Vertex, List<NeighborhoodVertex>>> getPerVertex(LogicalGraph graph,
                 DataSet<Vertex> centralVertices, EdgeDirection edgeDirection) {
    Objects.requireNonNull(graph);
    Objects.requireNonNull(centralVertices);
    Objects.requireNonNull(edgeDirection);
    DataSet<Tuple2<Vertex, List<NeighborhoodVertex>>> incoming = null;
    DataSet<Tuple2<Vertex, List<NeighborhoodVertex>>> outgoing = null;

    // get incoming
    if (edgeDirection.equals(EdgeDirection.INCOMING) ||
      edgeDirection.equals(EdgeDirection.UNDIRECTED)) {
      incoming = graph.getEdges()
        .coGroup(centralVertices)
        .where(new TargetId<>())
        .equalTo(new Id<>())
        .with(new CreateNeighborList(edgeDirection));
    }

    // get outgoing
    if (edgeDirection.equals(EdgeDirection.OUTGOING) ||
      edgeDirection.equals(EdgeDirection.UNDIRECTED)) {
      outgoing = graph.getEdges()
        .coGroup(centralVertices)
        .where(new SourceId<>())
        .equalTo(new Id<>())
        .with(new CreateNeighborList(edgeDirection));
    }

    if (edgeDirection.equals(EdgeDirection.UNDIRECTED)) {
      return incoming.union(outgoing);
    }
    return edgeDirection.equals(EdgeDirection.INCOMING) ? incoming : outgoing;
  }

  /**
   * A simple ENUM which contains possible edge directions viewed from the central vertex.
   */
  public enum EdgeDirection {
    /**
     * Can be used for edges starting from the neighbor to the central vertex.
     */
    INCOMING,

    /**
     * Can be used for edges starting from the central vertex to the neighbor.
     */
    OUTGOING,

    /**
     * Can be used if the edge direction should be ignored an INCOMING and OUTGOING edges should be
     * taken into account for the calculation.
     */
    UNDIRECTED
  }
}
