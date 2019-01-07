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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.model.api.entities.EPGMVertexFactory;
import org.gradoop.common.model.impl.id.GradoopId;

import java.util.Objects;

/**
 * A {@link MapFunction} that creates a new vertex based on the given edge. Furthermore it
 * returns the source and target id of the edge for later use.
 *
 * @param <V> The vertex type.
 * @param <E> The edge type.
 */
public class CreateVertexFromEdges<V extends EPGMVertex, E extends EPGMEdge>
  implements MapFunction<E, Tuple3<V, GradoopId, GradoopId>>,
  ResultTypeQueryable<Tuple3<V, GradoopId, GradoopId>> {

  /**
   * The factory vertices are created with.
   */
  private final EPGMVertexFactory<V> factory;

  /**
   * Reduce object instantiations.
   */
  private final Tuple3<V, GradoopId, GradoopId> reuse;

  /**
   * The constructor of the MapFunction.
   *
   * @param newVertexLabel The label of the newly created vertex.
   * @param factory        The factory for creating new vertices.
   */
  public CreateVertexFromEdges(String newVertexLabel, EPGMVertexFactory<V> factory) {
    this.factory = Objects.requireNonNull(factory);
    this.reuse = new Tuple3<>(factory.createVertex(newVertexLabel), null, null);
  }

  @Override
  public Tuple3<V, GradoopId, GradoopId> map(E e) {
    reuse.f0.setId(GradoopId.get());
    reuse.f0.setProperties(e.getProperties());
    reuse.f1 = e.getSourceId();
    reuse.f2 = e.getTargetId();
    return reuse;
  }

  @Override
  public TypeInformation<Tuple3<V, GradoopId, GradoopId>> getProducedType() {
    final TypeInformation<GradoopId> idType = TypeInformation.of(GradoopId.class);
    return new TupleTypeInfo<>(TypeInformation.of(factory.getType()), idType, idType);
  }
}
