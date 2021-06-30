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
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.temporal.model.api.TimeDimension;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;

import java.util.Objects;

/**
 * Map function to extract id and temporal interval from a vertex.
 */
@FunctionAnnotation.ForwardedFields("id->f0")
public class ExtractIdIntervalMap implements MapFunction<TemporalVertex, Tuple3<GradoopId, Long, Long>> {
  /**
   * Time dimension to consider.
   */
  private final TimeDimension dimension;
  /**
   * Reduce object instantiations.
   */
  private final Tuple3<GradoopId, Long, Long> reuse;

  /**
   * Creates an instance of this map transformation.
   *
   * @param dimension the time dimension to extract from the vertex.
   */
  public ExtractIdIntervalMap(TimeDimension dimension) {
    this.dimension = Objects.requireNonNull(dimension);
    this.reuse = new Tuple3<>();
  }

  @Override
  public Tuple3<GradoopId, Long, Long> map(TemporalVertex v) throws Exception {
    reuse.f0 = v.getId();
    reuse.f1 = dimension.equals(TimeDimension.VALID_TIME) ? v.getValidFrom() : v.getTxFrom();
    reuse.f2 = dimension.equals(TimeDimension.VALID_TIME) ? v.getValidTo() : v.getTxTo();
    return reuse;
  }
}
