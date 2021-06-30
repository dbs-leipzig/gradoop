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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.sampling.functions.VertexDegree;
import org.gradoop.temporal.model.api.TimeDimension;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;

import java.util.Objects;

/**
 * A flat map function extracting the id and temporal interval from an {@link TemporalEdge} instance.
 */
@FunctionAnnotation.ForwardedFields("id->f0")
public class FlatMapVertexIdEdgeInterval implements FlatMapFunction<TemporalEdge, Tuple3<GradoopId, Long, Long>> {

  /**
   * The time dimension to consider.
   */
  private final TimeDimension timeDimension;

  /**
   * The degree type to consider.
   */
  private final VertexDegree degreeType;

  /**
   * Creates an instance of this flat map transformation.
   *
   * @param timeDimension the time dimension to consider
   * @param degreeType the degree type to consider
   */
  public FlatMapVertexIdEdgeInterval(TimeDimension timeDimension, VertexDegree degreeType) {
    this.timeDimension = Objects.requireNonNull(timeDimension);
    this.degreeType = Objects.requireNonNull(degreeType);
  }

  @Override
  public void flatMap(TemporalEdge temporalEdge, Collector<Tuple3<GradoopId, Long, Long>> collector) throws
    Exception {
    Long from = timeDimension
      .equals(TimeDimension.VALID_TIME) ? temporalEdge.getValidFrom() : temporalEdge.getTxFrom();
    Long to = timeDimension
      .equals(TimeDimension.VALID_TIME) ? temporalEdge.getValidTo() : temporalEdge.getTxTo();
    switch (degreeType) {
    case IN:
      collector.collect(new Tuple3<>(temporalEdge.getTargetId(), from, to));
      break;
    case OUT:
      collector.collect(new Tuple3<>(temporalEdge.getSourceId(), from, to));
      break;
    case BOTH:
      collector.collect(new Tuple3<>(temporalEdge.getTargetId(), from, to));
      collector.collect(new Tuple3<>(temporalEdge.getSourceId(), from, to));
      break;
    default:
      throw new IllegalArgumentException("Invalid vertex degree type [" + degreeType + "].");
    }
  }
}
