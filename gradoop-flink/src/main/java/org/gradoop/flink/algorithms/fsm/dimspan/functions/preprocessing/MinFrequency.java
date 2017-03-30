/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.algorithms.fsm.dimspan.functions.preprocessing;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.flink.algorithms.fsm.dimspan.config.DIMSpanConfig;

import java.math.BigDecimal;

/**
 * Calculates the min frequency based on a configured min support.
 */
public class MinFrequency implements MapFunction<Long, Long> {

  /**
   * FSM configuration
   */
  private final DIMSpanConfig fsmConfig;

  /**
   * Constructor.
   * @param fsmConfig FSM configuration
   */
  public MinFrequency(DIMSpanConfig fsmConfig) {
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
