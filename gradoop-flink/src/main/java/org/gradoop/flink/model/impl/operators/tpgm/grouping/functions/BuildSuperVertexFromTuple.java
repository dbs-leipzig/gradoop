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
package org.gradoop.flink.model.impl.operators.tpgm.grouping.functions;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.model.api.entities.EPGMVertexFactory;
import org.gradoop.flink.model.api.functions.AggregateFunction;
import org.gradoop.flink.model.api.tpgm.functions.grouping.GroupingKeyFunction;

import java.util.List;
import java.util.Objects;

import static org.gradoop.flink.model.impl.operators.tpgm.grouping.functions.TemporalGroupingConstants.VERTEX_TUPLE_RESERVED;
import static org.gradoop.flink.model.impl.operators.tpgm.grouping.functions.TemporalGroupingConstants.VERTEX_TUPLE_SUPERID;

/**
 * Build the final super-vertex from the internal tuple-based representation.
 *
 * @param <T> The input tuple type.
 * @param <E> The final vertex type.
 */
public class BuildSuperVertexFromTuple<T extends Tuple, E extends EPGMVertex>
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
  public BuildSuperVertexFromTuple(List<GroupingKeyFunction<? super E, ?>> groupingKeys,
    List<AggregateFunction> aggregateFunctions, EPGMVertexFactory<E> vertexFactory) {
    super(VERTEX_TUPLE_RESERVED, groupingKeys, aggregateFunctions);
    reuse = Objects.requireNonNull(vertexFactory).createVertex();
    vertexType = vertexFactory.getType();
  }

  @Override
  public E map(T tuple) throws Exception {
    E vertex = setAggregatePropertiesAndKeys(reuse, tuple);
    vertex.setId(tuple.getField(VERTEX_TUPLE_SUPERID));
    return vertex;
  }

  @Override
  public TypeInformation<E> getProducedType() {
    return TypeInformation.of(vertexType);
  }
}
