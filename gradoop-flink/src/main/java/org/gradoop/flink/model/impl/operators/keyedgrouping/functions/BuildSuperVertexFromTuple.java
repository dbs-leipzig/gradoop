/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.operators.keyedgrouping.functions;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.common.model.api.entities.VertexFactory;
import org.gradoop.flink.model.api.functions.AggregateFunction;
import org.gradoop.flink.model.api.functions.KeyFunction;

import java.util.List;
import java.util.Objects;

/**
 * Build the final super-vertex from the internal tuple-based representation.
 *
 * @param <T> The input tuple type.
 * @param <E> The final vertex type.
 */
public class BuildSuperVertexFromTuple<T extends Tuple, E extends Vertex>
  extends BuildSuperElementFromTuple<T, E> {

  /**
   * The result vertex type.
   */
  private final Class<E> vertexType;

  /**
   * Reduce object instantiations.
   */
  private final E reuse;

  /**
   * Initialize this function.
   *
   * @param groupingKeys       The grouping key functions.
   * @param aggregateFunctions The aggregate functions.
   * @param vertexFactory      A factory used to create new vertices.
   */
  public BuildSuperVertexFromTuple(List<KeyFunction<E, ?>> groupingKeys,
    List<AggregateFunction> aggregateFunctions, VertexFactory<E> vertexFactory) {
    super(GroupingConstants.VERTEX_TUPLE_RESERVED, groupingKeys, aggregateFunctions);
    reuse = Objects.requireNonNull(vertexFactory).createVertex();
    vertexType = vertexFactory.getType();
  }

  @Override
  public E map(T tuple) throws Exception {
    E vertex = setAggregatePropertiesAndKeys(reuse, tuple);
    vertex.setId(tuple.getField(GroupingConstants.VERTEX_TUPLE_SUPERID));
    return vertex;
  }

  @Override
  public TypeInformation<E> getProducedType() {
    return TypeInformation.of(vertexType);
  }
}
