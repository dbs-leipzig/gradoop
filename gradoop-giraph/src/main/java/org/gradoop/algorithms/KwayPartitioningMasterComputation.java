package org.gradoop.algorithms;

import org.apache.giraph.aggregators.IntSumAggregator;
import org.apache.giraph.master.DefaultMasterCompute;

/**
 * Created by gomezk on 16.12.14.
 */
public class KwayPartitioningMasterComputation extends DefaultMasterCompute {
  /**
   * Creates as many types aggregators as defined in {@link
   * org.gradoop.algorithms.KwayPartitioningComputation} of type defined in
   * {@link org.gradoop.algorithms.KwayPartitioningComputation}
   *
   * @throws IllegalAccessException
   * @throws InstantiationException
   */
  @Override
  public void initialize() throws IllegalAccessException,
    InstantiationException {
    int partitionCount = Integer.valueOf(getConf()
      .get(KwayPartitioningComputation.NUMBER_OF_PARTITIONS,
        KwayPartitioningComputation.DEFAULT_PARTITIONS));
    Class aggregatorClass = getConf()
      .getClass(KwayPartitioningComputation.KWAY_AGGREGATOR_CLASS,
        IntSumAggregator.class);
    for (int i = 0; i < partitionCount; i++) {
      registerAggregator(KwayPartitioningComputation
        .KWAY_DEMAND_AGGREGATOR_PREFIX + i, aggregatorClass);
      registerAggregator(
        KwayPartitioningComputation.KWAY_CAPACITY_AGGREGATOR_PREFIX + i,
        aggregatorClass);
    }
  }

  @Override
  public void compute(){
    int iterations =  Integer.valueOf(getConf().get
      (KwayPartitioningComputation.NUMBER_OF_ITERATIONS));
    if(getSuperstep() == iterations) haltComputation();
  }
}
