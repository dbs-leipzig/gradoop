package org.gradoop.flink.algorithms.fsm.functions;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.flink.algorithms.fsm.tuples.FrequentSubgraph;
import org.gradoop.flink.algorithms.fsm.config.Constants;

/**
 * Created by peet on 09.09.16.
 */
public class ByFrequency extends RichFilterFunction<FrequentSubgraph> {


  private long minFrequency;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    this.minFrequency = getRuntimeContext()
      .<Long>getBroadcastVariable(Constants.MIN_FREQUENCY).get(0);
  }

  @Override
  public boolean filter(FrequentSubgraph subgraph) throws Exception {
    return subgraph.getFrequency() >= minFrequency;
  }
}
