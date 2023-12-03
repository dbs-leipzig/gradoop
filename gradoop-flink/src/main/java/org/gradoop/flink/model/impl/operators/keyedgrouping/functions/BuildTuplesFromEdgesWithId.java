/*
 * Copyright © 2014 - 2021 Leipzig University (Database Research Group)
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

import org.apache.flink.api.java.tuple.Tuple;
import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.flink.model.api.functions.AggregateFunction;
import org.gradoop.flink.model.api.functions.KeyFunction;

import java.util.List;

/**
 * Build a tuple-based representation of edges for grouping with an additional source ID field at position
 * {@value GroupingConstants#EDGE_TUPLE_ID}. All other fields will be shifted by
 * {@value GroupingConstants#EDGE_RETENTION_OFFSET}.
 *
 * @param <E> The edge type.
 */
public class BuildTuplesFromEdgesWithId<E extends Edge> extends BuildTuplesFromEdges<E> {

  /**
   * Initialize this function, setting the grouping keys and aggregate functions.
   *
   * @param keys               The edge grouping keys.
   * @param aggregateFunctions The edge aggregate functions.
   */
  public BuildTuplesFromEdgesWithId(List<KeyFunction<E, ?>> keys,
    List<AggregateFunction> aggregateFunctions) {
    super(keys, aggregateFunctions, GroupingConstants.EDGE_RETENTION_OFFSET);
  }

  @Override
  public Tuple map(E element) throws Exception {
    final Tuple tuple = super.map(element);
    tuple.setField(element.getId(), GroupingConstants.EDGE_TUPLE_ID);
    return tuple;
  }
}
