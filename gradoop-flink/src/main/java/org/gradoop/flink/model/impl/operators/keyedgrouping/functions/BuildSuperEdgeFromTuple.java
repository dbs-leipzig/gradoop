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
import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.api.entities.EdgeFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.api.functions.AggregateFunction;
import org.gradoop.flink.model.api.functions.KeyFunction;

import java.util.List;
import java.util.Objects;

/**
 * Build the final super-edge from the internal tuple-based representation.
 *
 * @param <T> The input tuple type.
 * @param <E> The final edge type.
 */
public class BuildSuperEdgeFromTuple<T extends Tuple, E extends Edge>
  extends BuildSuperElementFromTuple<T, E> {

  /**
   * The result edge type.
   */
  private final Class<E> edgeType;

  /**
   * Reduce object instantiations.
   */
  private final E reuse;

  /**
   * Initialize this function.
   *
   * @param groupingKeys       The grouping key functions.
   * @param aggregateFunctions The aggregate functions.
   * @param edgeFactory        A factory used to create new edges.
   */
  public BuildSuperEdgeFromTuple(List<KeyFunction<E, ?>> groupingKeys,
    List<AggregateFunction> aggregateFunctions, EdgeFactory<E> edgeFactory) {
    super(GroupingConstants.EDGE_TUPLE_RESERVED, groupingKeys, aggregateFunctions);
    reuse = Objects.requireNonNull(edgeFactory).createEdge(GradoopId.NULL_VALUE, GradoopId.NULL_VALUE);
    edgeType = edgeFactory.getType();
  }

  @Override
  public E map(T tuple) throws Exception {
    E edge = setAggregatePropertiesAndKeys(reuse, tuple);
    edge.setId(GradoopId.get());
    edge.setSourceId(tuple.getField(GroupingConstants.EDGE_TUPLE_SOURCEID));
    edge.setTargetId(tuple.getField(GroupingConstants.EDGE_TUPLE_TARGETID));
    return edge;
  }

  @Override
  public TypeInformation<E> getProducedType() {
    return TypeInformation.of(edgeType);
  }
}
