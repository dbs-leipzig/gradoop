package org.gradoop.flink.algorithms.fsm.gspan.functions;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.flink.algorithms.fsm.config.Constants;
import org.gradoop.flink.model.impl.tuples.WithCount;

/**
 * Created by peet on 09.09.16.
 */
public class Frequent<T> extends
  RichFilterFunction<WithCount<T>> {


  private long minFrequency;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    this.minFrequency = getRuntimeContext()
      .<Long>getBroadcastVariable(Constants.MIN_FREQUENCY).get(0);
  }

  @Override
  public boolean filter(WithCount<T> value) throws Exception {
    return value.getCount() >= minFrequency;
  }
}
