
package org.gradoop.flink.algorithms.fsm.dimspan.functions.mining;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.flink.algorithms.fsm.dimspan.config.DIMSpanConstants;
import org.gradoop.flink.model.api.tuples.Countable;

/**
 * Filters something countable by minimum frequency.
 *
 * @param <T> type of something
 */
public class Frequent<T extends Countable> extends RichFilterFunction<T> {

  /**
   * minimum frequency
   */
  private long minFrequency;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    this.minFrequency = getRuntimeContext()
      .<Long>getBroadcastVariable(DIMSpanConstants.MIN_FREQUENCY).get(0);
  }

  @Override
  public boolean filter(T value) throws Exception {
    return value.getCount() >= minFrequency;
  }
}
