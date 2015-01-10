package org.gradoop.algorithms;

import org.apache.giraph.aggregators.IntSumAggregator;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.log4j.Logger;

/**
 * Master computation for
 * {@link org.gradoop.algorithms.AdaptiveRepartitioningComputation}
 */
public class AdaptiveRepartitioningMasterComputation extends
  DefaultMasterCompute {
  /**
   * Class logger.
   */
  private static final Logger LOG =
    Logger.getLogger(AdaptiveRepartitioningMasterComputation.class);

  /**
   * Creates as many types aggregators as defined in {@link
   * AdaptiveRepartitioningComputation} of type defined in
   * {@link AdaptiveRepartitioningComputation}
   *
   * @throws IllegalAccessException
   * @throws InstantiationException
   */
  @Override
  public void initialize() throws IllegalAccessException,
    InstantiationException {
    int partitionCount = getConf()
      .getInt(AdaptiveRepartitioningComputation.NUMBER_OF_PARTITIONS,
        AdaptiveRepartitioningComputation.DEFAULT_NUMBER_OF_PARTITIONS);
    for (int i = 0; i < partitionCount; i++) {
      registerAggregator(
        AdaptiveRepartitioningComputation.DEMAND_AGGREGATOR_PREFIX + i,
        IntSumAggregator.class);
      registerPersistentAggregator(
        AdaptiveRepartitioningComputation.CAPACITY_AGGREGATOR_PREFIX + i,
        IntSumAggregator.class);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void compute() {
    int partitionCount = getConf()
      .getInt(AdaptiveRepartitioningComputation.NUMBER_OF_PARTITIONS,
        AdaptiveRepartitioningComputation.DEFAULT_NUMBER_OF_PARTITIONS);
    int iterations = getConf()
      .getInt(AdaptiveRepartitioningComputation.NUMBER_OF_ITERATIONS,
        AdaptiveRepartitioningComputation.DEFAULT_NUMBER_OF_ITERATIONS);
    if (getSuperstep() == iterations) {
      haltComputation();
    }

    LOG.info("=== master CAPACITY aggregators");
    for (int i = 0; i < partitionCount; i++) {
      LOG.info(i + ": " + getAggregatedValue(
        AdaptiveRepartitioningComputation.CAPACITY_AGGREGATOR_PREFIX + i));
    }

    LOG.info("=== master DEMAND aggregators");
    for (int i = 0; i < partitionCount; i++) {
      LOG.info(i + ": " + getAggregatedValue(
        AdaptiveRepartitioningComputation.DEMAND_AGGREGATOR_PREFIX + i));
    }
  }
}
