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

import java.math.BigDecimal;

/**
 * Calculates the min frequency based on a configured min support.
 */
public class MinFrequency implements MapFunction<Long, Long> {

  /**
   * FSM configuration
   */
  private final FSMConfig fsmConfig;

  /**
   * Constructor.
   * @param fsmConfig FSM configuration
   */
  public MinFrequency(FSMConfig fsmConfig) {
    this.fsmConfig = fsmConfig;
  }

  @Override
  public Long map(Long count) throws Exception {
    return BigDecimal.valueOf(count)
      .multiply(
        BigDecimal.valueOf(fsmConfig.getMinSupport())
          .setScale(2, BigDecimal.ROUND_HALF_UP)
      )
      .setScale(0, BigDecimal.ROUND_UP)
      .longValue();
  }
}
