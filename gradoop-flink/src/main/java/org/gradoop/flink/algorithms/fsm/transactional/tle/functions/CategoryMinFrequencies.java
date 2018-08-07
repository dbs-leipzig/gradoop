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

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.flink.algorithms.fsm.transactional.common.FSMConfig;

import java.util.Map;

/**
 * Calculates category min frequencies from category graph counts.
 */
public class CategoryMinFrequencies
  implements MapFunction<Map<String, Long>, Map<String, Long>> {

  /**
   * map function calculating FSM min frequency
   */
  private final MapFunction<Long, Long> minFrequency;

  /**
   * Constructor.
   *
   * @param fsmConfig FSM configuration
   */
  public CategoryMinFrequencies(FSMConfig fsmConfig) {
    minFrequency = new MinFrequency(fsmConfig);
  }

  @Override
  public Map<String, Long> map(Map<String, Long> value) throws Exception {

    for (Map.Entry<String, Long> entry : value.entrySet()) {
      entry.setValue(minFrequency.map(entry.getValue()));
    }

    return value;
  }
}
