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
package org.gradoop.flink.model.impl.operators.statistics.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.tuples.WithCount;

/**
 * Calculates the distance of max degree and the degree of each vertex
 *
 * {@inheritDoc}
 */
public class DegreeDistanceFunction extends RichMapFunction<WithCount<GradoopId>, Tuple1<Long>> {

  /**
   * Storing max degree from broadcast
   */
  private WithCount<GradoopId>  maxDegree;

  /**
   * Name of broadcast variable
   */
  private final String broadcastName;

  /**
   * Tuple for reuse storing distances
   */
  private Tuple1<Long> reuse;

  /**
   * Creates an instance of {@link DegreeDistanceFunction}
   * to calculate the distance of max degree degree and the degree of each vertex
   *
   * @param broadcastName name of broadcast variable
   */
  public DegreeDistanceFunction(String broadcastName) {
    this.broadcastName = broadcastName;
    this.reuse = new Tuple1<>();
  }

  /**
   * Function called to store broadcasted max degree.
   *
   * @param parameter parameter
   * @throws Exception throws any Exception
   */
  @Override
  public void open(Configuration parameter) throws Exception {
    super.open(parameter);
    maxDegree = getRuntimeContext()
      .<WithCount<GradoopId>>getBroadcastVariable(this.broadcastName)
      .get(0);
  }

  /**
   * Mapping function converting degree of vertex
   * to distance of max degree and degree of vertex
   *
   * @param value degree of vertex
   * @return degree distance
   * @throws Exception throws any Exception
   */
  @Override
  public Tuple1<Long> map(WithCount<GradoopId> value) throws Exception {
    reuse.f0 = maxDegree.f1 - value.f1;
    return reuse;
  }
}
