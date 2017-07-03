
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
