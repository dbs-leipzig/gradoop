/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.algorithms;

import org.apache.giraph.aggregators.IntSumAggregator;
import org.apache.giraph.master.DefaultMasterCompute;

/**
 * Master computation for
 * {@link org.gradoop.algorithms.AdaptiveRepartitioningComputation}
 */
public class AdaptiveRepartitioningMasterComputation extends
  DefaultMasterCompute {

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
    int iterations = getConf()
      .getInt(AdaptiveRepartitioningComputation.NUMBER_OF_ITERATIONS,
        AdaptiveRepartitioningComputation.DEFAULT_NUMBER_OF_ITERATIONS);
    if (getSuperstep() == iterations) {
      haltComputation();
    }
  }
}
