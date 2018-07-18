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
package org.gradoop.flink.algorithms.fsm.transactional.tle.functions;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.flink.algorithms.fsm.dimspan.config.DIMSpanConstants;
import org.gradoop.flink.algorithms.fsm.transactional.tle.tuples.CategoryCountableLabel;

import java.util.Map;

/**
 * Filters categorized labels by minimum category frequency.
 */
public class CategoryFrequent
  extends RichFilterFunction<CategoryCountableLabel> {

  /**
   * minimum frequency
   */
  private Map<String, Long> categoryMinFrequencies;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    this.categoryMinFrequencies = getRuntimeContext()
      .<Map<String, Long>>getBroadcastVariable(DIMSpanConstants.MIN_FREQUENCY).get(0);
  }

  @Override
  public boolean filter(CategoryCountableLabel value) throws Exception {
    return value.getCount() >= categoryMinFrequencies.get(value.getCategory());
  }
}
