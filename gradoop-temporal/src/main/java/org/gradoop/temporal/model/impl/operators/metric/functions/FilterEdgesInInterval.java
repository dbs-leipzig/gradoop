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
package org.gradoop.temporal.model.impl.operators.metric.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.common.model.impl.id.GradoopId;

import java.util.Objects;

/**
 * A filter function that finds all temporal edges that exist within a given time interval.
 */
public class FilterEdgesInInterval implements FilterFunction<Tuple4<GradoopId, Long, Long, Integer>> {
  /**
   * The vertex to be considered.
   */
  private final GradoopId vertexId;
  /**
   * The start of the interval.
   */
  private final Long queryFrom;
  /**
   * The end of the interval.
   */
  private final Long queryTo;

  /**
   * Creates an instance of this filter transformation.
   *
   * @param queryFrom the start of the interval
   * @param queryTo   the end of the interval
   * @param vertexId the ID of the vertex to consider
   */
  public FilterEdgesInInterval(Long queryFrom, Long queryTo, GradoopId vertexId) {
    this.queryFrom = Objects.requireNonNull(queryFrom);
    this.queryTo = Objects.requireNonNull(queryTo);
    this.vertexId = Objects.requireNonNull(vertexId);
  }

  @Override
  public boolean filter(Tuple4<GradoopId, Long, Long, Integer> edgeTuple) throws Exception {
    return edgeTuple.f1 < queryTo && edgeTuple.f2 > queryFrom && edgeTuple.f0.equals(vertexId);
  }
}

