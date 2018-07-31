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
package org.gradoop.flink.model.impl.operators.aggregation.functions.containment;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.flink.model.api.functions.AggregateFunction;
import org.gradoop.flink.model.impl.operators.aggregation.functions.bool.Or;

/**
 * superclass of aggregate and filter functions that check vertex or edge label
 * presence in a graph.
 *
 * Usage: First, aggregate and, second, filter using the same UDF instance.
 */
public abstract class HasLabel extends Or
  implements AggregateFunction, FilterFunction<GraphHead> {

  /**
   * label to check presence of
   */
  protected final String label;

  /**
   * Constructor.
   *
   * @param label label to check presence of
   */
  public HasLabel(String label) {
    this.label = label;
  }

  @Override
  public boolean filter(GraphHead graphHead) throws Exception {
    return graphHead.getPropertyValue(getAggregatePropertyKey()).getBoolean();
  }
}
