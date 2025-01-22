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
 * Build a tuple-based representation of edges for grouping.
 * Tuples will contain the source ID, the target ID, all grouping keys and all aggregate values.
 *
 * @param <E> The element type.
 */
public class BuildTuplesFromEdges<E extends Edge> extends BuildTuplesFromElements<E> {

  /**
   * An additional edge offset. All tuple accesses will be shifted by this value.
   */
  private final int offset;

  /**
   * Initialize this function, setting the grouping keys and aggregate functions.<p>
   * This constructor will consider additional reserved fields in the edge tuple.
   *
   * @param keys               The grouping keys.
   * @param aggregateFunctions The aggregate functions used to determine the aggregate property
   * @param additionalOffset   An additional number of fields to be reserved in edge tuples.
   */
  protected BuildTuplesFromEdges(List<KeyFunction<E, ?>> keys, List<AggregateFunction> aggregateFunctions,
    int additionalOffset) {
    super(GroupingConstants.EDGE_TUPLE_RESERVED + additionalOffset, keys, aggregateFunctions);
    if (additionalOffset < 0) {
      throw new IllegalArgumentException("Additional offset can not be negative: " + additionalOffset);
    }
    this.offset = additionalOffset;
  }

  /**
   * Initialize this function, setting the grouping keys and aggregate functions.
   *
   * @param keys               The grouping keys.
   * @param aggregateFunctions The aggregate functions used to determine the aggregate property
   */
  public BuildTuplesFromEdges(List<KeyFunction<E, ?>> keys, List<AggregateFunction> aggregateFunctions) {
    this(keys, aggregateFunctions, 0);
  }

  @Override
  public Tuple map(E element) throws Exception {
    final Tuple result = super.map(element);
    result.setField(element.getSourceId(), GroupingConstants.EDGE_TUPLE_SOURCEID + offset);
    result.setField(element.getTargetId(), GroupingConstants.EDGE_TUPLE_TARGETID + offset);
    return result;
  }
}
