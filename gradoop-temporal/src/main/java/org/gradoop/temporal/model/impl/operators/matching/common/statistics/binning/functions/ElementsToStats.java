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
package org.gradoop.temporal.model.impl.operators.matching.common.statistics.binning.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.gradoop.temporal.model.impl.operators.matching.common.statistics.binning.pojo.TemporalElementStats;
import org.gradoop.temporal.model.impl.pojo.TemporalElement;

import java.util.Set;

/**
 * Reduces a set of {@link TemporalElement} to a {@link TemporalElementStats} about this set.
 * It is assumed that all elements have the same label.
 * @param <T> type of element to reduce
 */
public class ElementsToStats<T extends TemporalElement> implements
  GroupReduceFunction<T, TemporalElementStats> {

  /**
   * List of numerical properties to consider
   */
  private Set<String> numericalProperties;
  /**
   * List of categorical properties to consider
   */
  private Set<String> categoricalProperties;

  /**
   * Creates a new ElementsToStats function considering only specified properties
   * @param numericalProperties numerical properties to consider
   * @param categoricalProperties categorical propberties to consider
   */
  public ElementsToStats(Set<String> numericalProperties, Set<String> categoricalProperties) {
    this.numericalProperties = numericalProperties;
    this.categoricalProperties = categoricalProperties;
  }

  /**
   * Creates a new ElementsToStats function considering all properties
   */
  public ElementsToStats() {
    this(null, null);
  }

  @Override
  public void reduce(Iterable<T> values, Collector<TemporalElementStats> out) throws Exception {
    TemporalElementStats stats = new TemporalElementStats(numericalProperties, categoricalProperties);
    // Reduce the collection by offering all elements to the reservoir-sample-based statistics
    for (TemporalElement element : values) {
      stats.addElement(element);
      stats.setLabel(element.getLabel());
    }
    out.collect(stats);
  }
}
