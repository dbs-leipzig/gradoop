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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.common.model.impl.id.GradoopId;

import java.util.Objects;

/**
 * A map function which calculates partial average degrees, an interim result for calculating the
 * overall average degree.
 */
public class MapCalculatePartialAverageDegree
  implements MapFunction<Tuple4<GradoopId, Long, Long, Integer>, Tuple2<GradoopId, Long>> {
  /**
   * The start of the interval specified by the user.
   */
  private final Long queryFrom;
  /**
   * The end of the interval specified by the user.
   */
  private final Long queryTo;

  /**
   * Creates an instance of this map transformation.
   *
   * @param queryFrom the start of the interval
   * @param queryTo   the end of the interval
   */
  public MapCalculatePartialAverageDegree(Long queryFrom, Long queryTo) {
    this.queryFrom = Objects.requireNonNull(queryFrom);
    this.queryTo = Objects.requireNonNull(queryTo);
  }

  @Override
  public Tuple2<GradoopId, Long> map(Tuple4<GradoopId, Long, Long, Integer> temporalEdge) throws Exception {
    if (temporalEdge.f1 < queryFrom) {
      temporalEdge.f1 = queryFrom;
    }
    if (temporalEdge.f2 > queryTo) {
      temporalEdge.f2 = queryTo;
    }
    Long lengthOfIntervalTimesDegree = (temporalEdge.f2 - temporalEdge.f1) * temporalEdge.f3;
    return new Tuple2<>(temporalEdge.f0, lengthOfIntervalTimesDegree);
  }
}
