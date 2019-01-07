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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMEdgeFactory;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.dataintegration.transformation.impl.NeighborhoodVertex;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * This function supports the calculation of the transitive closure in the direct neighborhood of a
 * vertex. For each transitive connection an edge is created containing the properties of the
 * central vertex and the labels of the first and second edge that need to be traversed for the
 * transitive connection.
 *
 * @param <V> The vertex type.
 * @param <E> The edge type.
 */
public class EdgesFromLocalTransitiveClosure<V extends EPGMVertex, E extends EPGMEdge>
  implements CoGroupFunction<Tuple2<V, List<NeighborhoodVertex>>,
    Tuple2<V, List<NeighborhoodVertex>>, E>, ResultTypeQueryable<E> {

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
   * The type of the edges created by the factory..
   */
  private final Class<E> edgeType;

  /**
   * Reduce object instantiations.
   */
  private final E reuse;

  /**
   * The constructor of the CoGroup function to created new edges based on transitivity.
   *
   * @param newEdgeLabel The edge label of the newly created edge.
   * @param factory The {@link EdgeFactory} new edges are created with.
   */
  public EdgesFromLocalTransitiveClosure(String newEdgeLabel, EPGMEdgeFactory<E> factory) {
    this.edgeType = Objects.requireNonNull(factory).getType();
    this.reuse = factory.createEdge(Objects.requireNonNull(newEdgeLabel), GradoopId.NULL_VALUE,
      GradoopId.NULL_VALUE);
  }

  @Override
  public void coGroup(Iterable<Tuple2<V, List<NeighborhoodVertex>>> incoming,
                      Iterable<Tuple2<V, List<NeighborhoodVertex>>> outgoing,
                      Collector<E> edges) {

    Iterator<Tuple2<V, List<NeighborhoodVertex>>> incIt = incoming.iterator();
    Iterator<Tuple2<V, List<NeighborhoodVertex>>> outIt = outgoing.iterator();

    if (incIt.hasNext() && outIt.hasNext()) {
      // each of the incoming and outgoing sets should be represented only once.
      Tuple2<V, List<NeighborhoodVertex>> first = incIt.next();
      V centralVertex = first.f0;
      List<NeighborhoodVertex> in = first.f1;
      List<NeighborhoodVertex> out = outIt.next().f1;
      if (in.isEmpty()) {
        return;
      }
      reuse.setProperties(centralVertex.getProperties());
      reuse.setProperty(ORIGINAL_VERTEX_LABEL, centralVertex.getLabel());

      for (NeighborhoodVertex source : in) {
        for (NeighborhoodVertex target : out) {
          reuse.setId(GradoopId.get());
          reuse.setSourceId(source.getNeighborId());
          reuse.setTargetId(target.getNeighborId());

          reuse.setProperty(FIRST_EDGE_LABEL, source.getConnectingEdgeLabel());
          reuse.setProperty(SECOND_EDGE_LABEL, target.getConnectingEdgeLabel());

          edges.collect(reuse);
        }
      }
    }
  }

  @Override
  public TypeInformation<E> getProducedType() {
    return TypeInformation.of(edgeType);
  }
}
