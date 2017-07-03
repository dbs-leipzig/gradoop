
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
