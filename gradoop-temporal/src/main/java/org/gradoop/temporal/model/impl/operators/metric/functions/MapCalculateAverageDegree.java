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
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;

import java.util.Objects;

/**
 * A map transformation that helps to calculate the average degree of a vertex.
 */
public class MapCalculateAverageDegree implements MapFunction<Tuple2<GradoopId, Long>, Tuple1<Double>> {
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
  public MapCalculateAverageDegree(Long queryFrom, Long queryTo) {
    this.queryFrom = Objects.requireNonNull(queryFrom);
    this.queryTo = Objects.requireNonNull(queryTo);
  }

  @Override
  public Tuple1<Double> map(Tuple2<GradoopId, Long> aggregatedTuple) throws Exception {
    Double degree = Double.valueOf(aggregatedTuple.f1) /
      (Double.valueOf(queryTo) - Double.valueOf(queryFrom));
    return new Tuple1<>(degree);
  }
}
