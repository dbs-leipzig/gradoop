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
package org.gradoop.temporal.model.impl.operators.matching.common.statistics.dummy;

import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.operators.matching.common.statistics.TemporalGraphStatistics;
import org.gradoop.temporal.model.impl.operators.matching.common.statistics.TemporalGraphStatisticsFactory;

import java.util.Set;

/**
 * Factory for {@link DummyTemporalGraphStatistics}
 */
public class DummyTemporalGraphStatisticsFactory implements TemporalGraphStatisticsFactory {

  @Override
  public TemporalGraphStatistics fromGraph(TemporalGraph g) {
    return new DummyTemporalGraphStatistics();
  }

  @Override
  public TemporalGraphStatistics fromGraphWithSampling(TemporalGraph g, int sampleSize) {
    return new DummyTemporalGraphStatistics();
  }

  @Override
  public TemporalGraphStatistics fromGraphWithSampling(TemporalGraph g, int sampleSize,
                                                       Set numericalProperties, Set categoricalProperties) {
    return new DummyTemporalGraphStatistics();
  }

  @Override
  public TemporalGraphStatistics fromGraph(TemporalGraph g, Set numericalProperties,
                                           Set categoricalProperties) {
    return new DummyTemporalGraphStatistics();
  }
}
