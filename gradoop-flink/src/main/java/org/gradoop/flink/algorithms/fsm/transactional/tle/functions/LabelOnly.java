
package org.gradoop.flink.algorithms.fsm.transactional.tle.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.flink.algorithms.fsm.transactional.tle.tuples.CategoryCountableLabel;

/**
 * (category, label, frequency) => label
 */
public class LabelOnly implements MapFunction<CategoryCountableLabel, String> {

  @Override
  public String map(CategoryCountableLabel value) throws Exception {
    return value.getLabel();
  }
}
