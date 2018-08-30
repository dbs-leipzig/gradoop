/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.operators.sampling.statistics.functions;

import org.apache.flink.api.common.functions.CrossFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.flink.model.impl.tuples.WithCount;

/**
 * Writes the sum of vertex degrees as property to the graphHead.
 */
public class AddSumDegreesToGraphHeadCrossFunction
  implements CrossFunction<WithCount<GradoopId>, GraphHead, GraphHead> {

  /**
   * The used property key for the sum of vertex degrees
   */
  private final String propertyKey;

  /**
   * Constructor
   *
   * @param propertyKey The used property key for the sum of vertex degrees
   */
  public AddSumDegreesToGraphHeadCrossFunction(String propertyKey) {
    this.propertyKey = propertyKey;
  }

  /**
   * Writes the sum of vertex degrees as property to the graphHead
   *
   * @param gradoopIdWithCount The {@code WithCount}-Object containing the sum-value
   * @param graphHead The graphHead the sum-value is written to
   * @return The graphHead with the sum-value as property
   */
  @Override
  public GraphHead cross(WithCount<GradoopId> gradoopIdWithCount, GraphHead graphHead) {
    graphHead.setProperty(propertyKey, gradoopIdWithCount.getCount());
    return graphHead;
  }
}
