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

import org.apache.flink.api.java.tuple.Tuple;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.flink.model.api.functions.AggregateFunction;
import org.gradoop.flink.model.api.tpgm.functions.grouping.GroupingKeyFunction;

import java.util.List;

import static org.gradoop.flink.model.impl.operators.tpgm.grouping.functions.TemporalGroupingConstants.EDGE_TUPLE_RESERVED;
import static org.gradoop.flink.model.impl.operators.tpgm.grouping.functions.TemporalGroupingConstants.EDGE_TUPLE_SOURCEID;
import static org.gradoop.flink.model.impl.operators.tpgm.grouping.functions.TemporalGroupingConstants.EDGE_TUPLE_TARGETID;

/**
 * Build a tuple-based representation of edges for grouping.
 * Tuples will contain the source ID, the target ID, all grouping keys and all aggregate values.
 *
 * @param <E> The element type.
 */
public class BuildTuplesFromEdges<E extends EPGMEdge> extends BuildTuplesFromElements<E> {

  /**
   * Initialize this function, setting the grouping keys and aggregate functions.
   *
   * @param keys               The grouping keys.
   * @param aggregateFunctions The aggregate functions used to determine the aggregate property
   */
  public BuildTuplesFromEdges(List<GroupingKeyFunction<? super E, ?>> keys,
    List<AggregateFunction> aggregateFunctions) {
    super(EDGE_TUPLE_RESERVED, keys, aggregateFunctions);
  }

  @Override
  public Tuple map(E element) throws Exception {
    final Tuple result = super.map(element);
    result.setField(element.getSourceId(), EDGE_TUPLE_SOURCEID);
    result.setField(element.getTargetId(), EDGE_TUPLE_TARGETID);
    return result;
  }
}
