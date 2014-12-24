/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
