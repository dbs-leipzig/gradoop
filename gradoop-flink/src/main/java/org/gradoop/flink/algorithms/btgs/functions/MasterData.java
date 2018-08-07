/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.algorithms.btgs.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.flink.algorithms.btgs.BusinessTransactionGraphs;

/**
 * Filters transactional vertices.
 * @param <V> vertex type.
 */
public class MasterData<V extends EPGMVertex> implements FilterFunction<V> {

  @Override
  public boolean filter(V v) throws Exception {
    return v.getPropertyValue(BusinessTransactionGraphs.SUPERTYPE_KEY)
      .getString().equals(
        BusinessTransactionGraphs.SUPERCLASS_VALUE_MASTER);
  }
}
