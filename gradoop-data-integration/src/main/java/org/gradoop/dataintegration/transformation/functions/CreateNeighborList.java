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
package org.gradoop.dataintegration.transformation.functions;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * This CoGroup operation creates a list of neighbors for each vertex.
 */
public class CreateNeighborList implements CoGroupFunction<Edge, Vertex, Tuple2<Vertex, List<NeighborhoodVertex>>> {

  /**
   * The edge direction to consider.
   */
  private final Neighborhood.EdgeDirection edgeDirection;

  /**
   * The constructor for the creation of neighbor lists.
   *
   * @param dir The edge direction to consider.
   */
  public CreateNeighborList(Neighborhood.EdgeDirection dir) {
    this.edgeDirection = dir;
  }

  @Override
  public void coGroup(Iterable<Edge> edges, Iterable<Vertex> vertex,
                      Collector<Tuple2<Vertex, List<NeighborhoodVertex>>> out) {
    // should only contain one or no vertex
    Iterator<Vertex> vertexIter = vertex.iterator();
    if (vertexIter.hasNext()) {
      Vertex v = vertexIter.next();

      List<NeighborhoodVertex> neighbors = new ArrayList<>();
      for (Edge e : edges) {
        neighbors.add(new NeighborhoodVertex(getNeighborId(v, e), e.getId(), e.getLabel()));
      }

      out.collect(Tuple2.of(v, neighbors));
    }
  }

  /**
   * Based on the considered edge direction the neighbor id is returned.
   *
   * @param v The vertex the neighbor id is searched for.
   * @param e The edge the neighbor id is taken from.
   * @return The GradoopId of the neighbor vertex.
   */
  private GradoopId getNeighborId(Vertex v, Edge e) {
    if (edgeDirection.equals(Neighborhood.EdgeDirection.INCOMING)) {
      return e.getSourceId();
    } else if (edgeDirection.equals(Neighborhood.EdgeDirection.OUTGOING)) {
      return e.getTargetId();
    }
    // for the undirected case we return the other id
    return v.getId().equals(e.getSourceId()) ? e.getTargetId() : e.getSourceId();
  }
}
