/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.operators.aggregation.functions.count;

import org.gradoop.flink.model.api.functions.EdgeAggregateFunction;

/**
 * Aggregate function returning the edge count of a graph / graph collection.
 */
public class EdgeCount extends Count implements EdgeAggregateFunction {

  /**
   * Creates a new instance of a EdgeCount aggregate function.
   */
  public EdgeCount() {
    super("edgeCount");
  }

  /**
   * Creates a new instance of a EdgeCount aggregate function.
   *
   * @param aggregatePropertyKey aggregate property key
   */
  public EdgeCount(String aggregatePropertyKey) {
    super(aggregatePropertyKey);
  }
}
