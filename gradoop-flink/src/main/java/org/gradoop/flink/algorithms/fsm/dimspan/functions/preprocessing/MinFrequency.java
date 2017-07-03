
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
