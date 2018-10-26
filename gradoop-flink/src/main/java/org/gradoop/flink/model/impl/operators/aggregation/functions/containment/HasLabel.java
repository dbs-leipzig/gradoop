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

import org.gradoop.flink.model.api.functions.ElementAggregateFunction;

/**
 * Aggregate and filter function to check presence of an element label in a graph.
 *
 * Usage: First, aggregate and, second, filter using the same UDF instance.
 */
public class HasLabel extends BaseHasLabel implements ElementAggregateFunction {

  /**
   * Creates a new instance of a HasLabel aggregate function.
   *
   * @param label element label to check presence of
   */
  public HasLabel(String label) {
    super(label, "hasLabel_" + label);
  }

  /**
   * Creates a new instance of a HasLabel aggregate function..
   *
   * @param label element label to check presence of
   * @param aggregatePropertyKey aggregate property key
   */
  public HasLabel(String label, String aggregatePropertyKey) {
    super(label, aggregatePropertyKey);
  }
}
