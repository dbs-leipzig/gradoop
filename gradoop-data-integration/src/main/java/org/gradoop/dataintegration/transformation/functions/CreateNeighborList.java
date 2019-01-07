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
package org.gradoop.dataintegration.transformation.functions;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.dataintegration.transformation.impl.Neighborhood;
import org.gradoop.dataintegration.transformation.impl.NeighborhoodVertex;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * This CoGroup operation creates a list of neighbors for each vertex.
 */
public class CreateNeighborList
  implements CoGroupFunction<Edge, Vertex, Tuple2<Vertex, List<NeighborhoodVertex>>> {

  /**
   * The edge direction to consider.
   */
  private final Neighborhood.EdgeDirection edgeDirection;

  /**
   * Reduce object instantiations.
   */
  private final Tuple2<Vertex, List<NeighborhoodVertex>> reuse;

  /**
   * The constructor for the creation of neighbor lists.
   *
   * @param edgeDirection The edge direction to consider.
   */
  public CreateNeighborList(Neighborhood.EdgeDirection edgeDirection) {
    this.edgeDirection = Objects.requireNonNull(edgeDirection);
    reuse = new Tuple2<>();
  }

  @Override
  public void coGroup(Iterable<Edge> edges, Iterable<Vertex> vertex,
                      Collector<Tuple2<Vertex, List<NeighborhoodVertex>>> out) {
    // should only contain one or no vertex
    Iterator<Vertex> vertexIterator = vertex.iterator();
    if (vertexIterator.hasNext()) {
      Vertex v = vertexIterator.next();

      List<NeighborhoodVertex> neighbors = new ArrayList<>();
      for (Edge e : edges) {
        neighbors.add(new NeighborhoodVertex(getNeighborId(v.getId(), e), e.getLabel()));
      }

      reuse.f0 = v;
      reuse.f1 = neighbors;
      out.collect(reuse);
    }
  }

  /**
   * Based on the considered edge direction the neighbor id is returned.
   *
   * @param vertexId The vertex id the neighbor id is searched for.
   * @param edge     The edge the neighbor id is taken from.
   * @return The GradoopId of the neighbor vertex.
   */
  private GradoopId getNeighborId(GradoopId vertexId, Edge edge) {
    switch (edgeDirection) {
    case INCOMING:
      return edge.getSourceId();
    case OUTGOING:
      return edge.getTargetId();
    default:
      return vertexId.equals(edge.getSourceId()) ? edge.getTargetId() : edge.getSourceId();
    }
  }
}
