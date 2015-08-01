package org.gradoop.algorithms;

import org.apache.giraph.master.DefaultMasterCompute;

/**
 * Master Computation for LabelPropagationComputation
 */
public class LabelPropagationMasterComputation extends DefaultMasterCompute {

  /**
   * {@inheritDoc}
   */
  @Override
  public void compute() {
    int iterations = getConf()
      .getInt(LabelPropagationComputation.NUMBER_OF_ITERATIONS,
        LabelPropagationComputation.DEFAULT_NUMBER_OF_ITERATIONS);
    if (getSuperstep() == iterations) {
      haltComputation();
    }
  }

}
