/*
 * Copyright © 2014 - 2021 Leipzig University (Database Research Group)
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
package org.gradoop.flink.algorithms.fsm.transactional.common.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;

/**
 * {@code (g, V, E) => true, if E not empty}
 */
public class NotEmpty implements FilterFunction<GraphTransaction> {

  @Override
  public boolean filter(GraphTransaction graphTransaction) throws Exception {
    return !graphTransaction.getEdges().isEmpty();
  }
}
