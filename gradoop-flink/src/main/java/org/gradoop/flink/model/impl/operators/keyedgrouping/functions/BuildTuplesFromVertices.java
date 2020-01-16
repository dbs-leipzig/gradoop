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

import org.apache.flink.api.java.tuple.Tuple;
import org.gradoop.common.model.api.entities.Element;
import org.gradoop.flink.model.api.functions.AggregateFunction;
import org.gradoop.flink.model.api.functions.KeyFunction;

import java.util.List;

/**
 * Build a tuple-based representation of vertices for grouping.
 * Tuples will contain the vertex ID, a reserved field for the super vertex ID, all grouping keys
 * and all aggregate values.
 *
 * @param <E> The element type.
 */
public class BuildTuplesFromVertices<E extends Element> extends BuildTuplesFromElements<E> {

  /**
   * Initialize this function, setting the grouping keys abd aggregate functions.
   *
   * @param keys               The grouping keys.
   * @param aggregateFunctions The aggregate functions used to determine the aggregate property
   */
  public BuildTuplesFromVertices(List<KeyFunction<E, ?>> keys, List<AggregateFunction> aggregateFunctions) {
    super(GroupingConstants.VERTEX_TUPLE_RESERVED, keys, aggregateFunctions);
  }

  @Override
  public Tuple map(E element) throws Exception {
    final Tuple result = super.map(element);
    result.setField(element.getId(), GroupingConstants.VERTEX_TUPLE_ID);
    return result;
  }
}
