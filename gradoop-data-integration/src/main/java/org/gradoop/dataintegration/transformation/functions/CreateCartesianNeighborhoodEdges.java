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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMEdgeFactory;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.dataintegration.transformation.impl.NeighborhoodVertex;

import java.util.List;
import java.util.Objects;

/**
 * This {@link FlatMapFunction} creates all edges between neighbor vertices.
 *
 * @param <V> The vertex type.
 * @param <E> The edge type.
 * @see org.gradoop.dataintegration.transformation.ConnectNeighbors
 */
@FunctionAnnotation.ReadFields({"f1"})
public class CreateCartesianNeighborhoodEdges<V extends EPGMVertex, E extends EPGMEdge>
  implements FlatMapFunction<Tuple2<V, List<NeighborhoodVertex>>, E>, ResultTypeQueryable<E> {

  /**
   * The type of the edges created by the factory.
   */
  private final Class<E> edgeType;

  /**
   * Reduce object instantiations.
   */
  private E reuseEdge;

  /**
   * The constructor to calculate the edges in the neighborhood.
   *
   * @param factory      The factory the edges are created with.
   * @param newEdgeLabel The label of the created edge between the neighbors.
   */
  public CreateCartesianNeighborhoodEdges(EPGMEdgeFactory<E> factory, String newEdgeLabel) {
    this.edgeType = Objects.requireNonNull(factory).getType();
    this.reuseEdge = factory.createEdge(Objects.requireNonNull(newEdgeLabel),
      GradoopId.NULL_VALUE, GradoopId.NULL_VALUE);
  }

  @Override
  public void flatMap(Tuple2<V, List<NeighborhoodVertex>> value, Collector<E> out) {
    final List<NeighborhoodVertex> neighbors = value.f1;

    // To "simulate" bidirectional edges we have to create an edge for each direction.
    for (NeighborhoodVertex source : neighbors) {
      // The source id is the same for the inner loop, we can keep it.
      reuseEdge.setSourceId(source.getNeighborId());
      for (NeighborhoodVertex target : neighbors) {
        if (source == target) {
          continue;
        }
        reuseEdge.setId(GradoopId.get());
        reuseEdge.setTargetId(target.getNeighborId());
        out.collect(reuseEdge);
      }
    }
  }

  @Override
  public TypeInformation<E> getProducedType() {
    return TypeInformation.of(edgeType);
  }
}
