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
package org.gradoop.temporal.model.impl.operators.metric;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.sampling.functions.VertexDegree;
import org.gradoop.temporal.model.api.TimeDimension;
import org.gradoop.temporal.model.impl.operators.metric.functions.AggregationType;

/**
 * A TPGM operator calculating the minimum degree of a given vertex referenced via its {@code vertexId}
 * within a given time interval: start {@code queryFrom}, end {@code queryTo}. The result is a single
 * value (Double). The logic is implemented in the superclass {@link BaseVertexCentricDegreeEvolution}.
 * <p>
 * The type of the degree (IN, OUT, BOTH) can be chosen by the arguments.
 */
public class VertexCentricMinDegreeEvolution extends BaseVertexCentricDegreeEvolution {
  /**
   * Creates an instance of this temporal vertex-centric minimum degree aggregation operator.
   *
   * @param degreeType the degree type to consider
   * @param dimension  the time dimension to consider
   * @param vertexId   the id of the vertex to consider
   * @param queryFrom  the start of the interval
   * @param queryTo    the end of the interval
   */
  public VertexCentricMinDegreeEvolution(VertexDegree degreeType, TimeDimension dimension,
                                         GradoopId vertexId, Long queryFrom, Long queryTo) {
    super(degreeType, dimension, vertexId, queryFrom, queryTo, AggregationType.MIN);
  }
}
