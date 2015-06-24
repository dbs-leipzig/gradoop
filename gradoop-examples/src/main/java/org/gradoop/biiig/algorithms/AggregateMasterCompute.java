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

package org.gradoop.biiig.algorithms;

import org.apache.giraph.aggregators.IntSumAggregator;
import org.apache.giraph.master.DefaultMasterCompute;

/**
 * Simple master computation example.
 */
public class AggregateMasterCompute extends DefaultMasterCompute {

  /**
   * Creates as many types aggregators as defined in {@link
   * AggregateComputation} of type defined in {@link
   * AggregateComputation}
   *
   * @throws IllegalAccessException
   * @throws InstantiationException
   */
  @Override
  public void initialize() throws IllegalAccessException,
    InstantiationException {
    long btgCount = getConf().getLong(AggregateComputation.BTG_AGGREGATOR_CNT,
      AggregateComputation.DEFAULT_BTG_CNT);
    Class aggregatorClass = getConf()
      .getClass(AggregateComputation.BTG_AGGREGATOR_CLASS,
        IntSumAggregator.class);
    for (int i = 0; i < btgCount; i++) {
      registerAggregator(AggregateComputation.BTG_AGGREGATOR_PREFIX + i,
        aggregatorClass);
    }
  }
}
