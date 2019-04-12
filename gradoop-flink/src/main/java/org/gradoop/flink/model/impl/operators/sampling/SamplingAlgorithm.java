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
package org.gradoop.flink.model.impl.operators.sampling;

import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;

/**
 * Abstract class to provide methods for sampling algorithms.
 */
public abstract class SamplingAlgorithm implements UnaryGraphToGraphOperator {

  @Override
  public LogicalGraph execute(LogicalGraph graph) {
    return sample(graph);
  }

  /**
   * Each sampling method should prepare a sample method
   *
   * @param graph graph that will be sampled of
   * @return sampled graph
   */
  protected abstract LogicalGraph sample(LogicalGraph graph);
}
