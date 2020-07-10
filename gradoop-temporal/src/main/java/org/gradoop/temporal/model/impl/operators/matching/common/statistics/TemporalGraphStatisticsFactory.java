/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
package org.gradoop.temporal.model.impl.operators.matching.common.statistics;

import org.gradoop.temporal.model.impl.TemporalGraph;

/**
 * Abstract base class for factories for {@link TemporalGraphStatistics}
 * @param <T> the type of {@link TemporalGraphStatistics}
 */
public interface TemporalGraphStatisticsFactory<T extends TemporalGraphStatistics> {

  /**
   * Create statistics from a temporal graph
   * @param g graph to create statistics for
   * @return graph statistics
   */
  T fromGraph(TemporalGraph g);

  /**
   * Create statistics from a temporal graph based only on a sample of it
   * @param g graph to create statistics for
   * @param sampleSize size of sample to be used
   * @return graph statistics
   */
  T fromGraphWithSampling(TemporalGraph g, int sampleSize);
}
