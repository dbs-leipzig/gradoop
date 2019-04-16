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
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMEdgeFactory;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.model.impl.id.GradoopId;

import java.util.Objects;

/**
 * A {@link FlatMapFunction} to create two new edges per inserted edge.
 * Source to new vertex, new vertex to target.
 *
 * @param <V> The vertex type.
 * @param <E> The edge type.
 */
public class CreateEdgesFromTriple<V extends EPGMVertex, E extends EPGMEdge>
  implements FlatMapFunction<Tuple3<V, GradoopId, GradoopId>, E>, ResultTypeQueryable<E> {

  /**
   * The edge type created by the factory.
   */
  private final Class<E> edgeType;

  /**
   * The label of the newly created edge which points to the newly created vertex.
   */
  private final String edgeLabelSourceToNew;

  /**
   * The label of the newly created edge which starts at the newly created vertex.
   */
  private final String edgeLabelNewToTarget;

  /**
   * Reduce object instantiations.
   */
  private E reuse;

  /**
   * The constructor to create the new edges based on the given triple.
   *
   * @param factory              The Factory which creates the new edges.
   * @param edgeLabelSourceToNew The label of the newly created edge which points to the newly
   *                             created vertex.
   * @param edgeLabelNewToTarget The label of the newly created edge which starts at the newly
   *                             created vertex.
   */
  public CreateEdgesFromTriple(EPGMEdgeFactory<E> factory, String edgeLabelSourceToNew,
    String edgeLabelNewToTarget) {
    this.edgeType = Objects.requireNonNull(factory).getType();
    this.edgeLabelSourceToNew = Objects.requireNonNull(edgeLabelSourceToNew);
    this.edgeLabelNewToTarget = Objects.requireNonNull(edgeLabelNewToTarget);
    this.reuse = factory.createEdge(edgeLabelSourceToNew, GradoopId.NULL_VALUE,
      GradoopId.NULL_VALUE);
  }

  @Override
  public void flatMap(Tuple3<V, GradoopId, GradoopId> triple, Collector<E> out) {
    reuse.setId(GradoopId.get());
    reuse.setLabel(edgeLabelSourceToNew);
    reuse.setSourceId(triple.f1);
    reuse.setTargetId(triple.f0.getId());
    out.collect(reuse);

    reuse.setId(GradoopId.get());
    reuse.setLabel(edgeLabelNewToTarget);
    reuse.setSourceId(triple.f0.getId());
    reuse.setTargetId(triple.f2);
    out.collect(reuse);
  }

  @Override
  public TypeInformation<E> getProducedType() {
    return TypeInformation.of(edgeType);
  }
}
