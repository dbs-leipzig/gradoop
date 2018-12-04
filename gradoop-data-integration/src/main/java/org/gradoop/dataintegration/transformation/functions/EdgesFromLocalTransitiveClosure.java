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
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.common.model.impl.pojo.Vertex;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * This function supports the calculation of the transitive closure in the direct neighborhood of a
 * vertex. For each transitive connection an edge is created containing the properties of the
 * central vertex and the labels of the first and second edge that need to be traversed for the
 * transitive connection.
 */
public class EdgesFromLocalTransitiveClosure implements CoGroupFunction<Tuple2<Vertex,
  List<Neighborhood.VertexPojo>>, Tuple2<Vertex, List<Neighborhood.VertexPojo>>, Edge> {

  /**
   * The property key used to store the original vertex label on the new edge.
   */
  public static final String ORIGINAL_VERTEX_LABEL = "originalVertexLabel";

  /**
   * The property key used to store the first label of the combined edges on the new edge.
   */
  public static final String FIRST_EDGE_LABEL = "firstEdgeLabel";

  /**
   * The property key used to store the second label of the combined edges on the new edge.
   */
  public static final String SECOND_EDGE_LABEL = "secondEdgeLabel";

  /**
   * The edge label of the newly created edge.
   */
  private final String newEdgeLabel;

  /**
   * The EdgeFactory new edges are created with.
   */
  private final EdgeFactory factory;

  /**
   * The constructor of the CoGroup function to created new edges based on transitivity.
   *
   * @param newEdgeLabel The edge label of the newly created edge.
   * @param factory The {@link EdgeFactory} new edges are created with.
   */
  public EdgesFromLocalTransitiveClosure(String newEdgeLabel, EdgeFactory factory) {
    Objects.requireNonNull(newEdgeLabel);
    Objects.requireNonNull(factory);
    this.newEdgeLabel = newEdgeLabel;
    this.factory = factory;
  }

  @Override
  public void coGroup(Iterable<Tuple2<Vertex, List<Neighborhood.VertexPojo>>> incoming,
                      Iterable<Tuple2<Vertex, List<Neighborhood.VertexPojo>>> outgoing,
                      Collector<Edge> edges) {

    Iterator<Tuple2<Vertex, List<Neighborhood.VertexPojo>>> incIt = incoming.iterator();
    Iterator<Tuple2<Vertex, List<Neighborhood.VertexPojo>>> outIt = outgoing.iterator();

    if (incIt.hasNext() && outIt.hasNext()) {
      // each of the incoming and outgoing sets should be represented only once.
      Tuple2<Vertex, List<Neighborhood.VertexPojo>> first = incIt.next();
      Vertex centralVertex = first.f0;
      List<Neighborhood.VertexPojo> in = first.f1;
      List<Neighborhood.VertexPojo> out = outIt.next().f1;

      for (Neighborhood.VertexPojo source : in) {
        for (Neighborhood.VertexPojo target: out) {
          Edge newEdge = factory
              .createEdge(newEdgeLabel, source.getNeighborId(), target.getNeighborId(),
                  centralVertex.getProperties());

          newEdge.setProperty(ORIGINAL_VERTEX_LABEL, centralVertex.getLabel());
          newEdge.setProperty(FIRST_EDGE_LABEL, source.getConnectingEdgeLabel());
          newEdge.setProperty(SECOND_EDGE_LABEL, target.getConnectingEdgeLabel());

          edges.collect(newEdge);
        }
      }
    }
  }
}
