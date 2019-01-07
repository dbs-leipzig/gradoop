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
package org.gradoop.flink.model.impl.operators.aggregation.functions.containment;

import org.gradoop.flink.model.api.functions.VertexAggregateFunction;

/**
 * Aggregate and filter function to check presence of a vertex label in a graph.
 *
 * <pre>
 * Usage:
 * <ol>
 * <li>aggregate
 * <li>filter using the same UDF instance.
 * </ol>
 * </pre>
 */
public class HasVertexLabel extends HasLabel implements VertexAggregateFunction {

  /**
   * Creates a new instance of a HasVertexLabel aggregate function.
   *
   * @param label vertex label to check presence of
   */
  public HasVertexLabel(String label) {
    super(label, "hasVertexLabel_" + label);
  }

  /**
   * Creates a new instance of a HasVertexLabel aggregate function.
   *
   * @param label vertex label to check presence of
   * @param aggregatePropertyKey aggregate property key
   */
  public HasVertexLabel(String label, String aggregatePropertyKey) {
    super(label, aggregatePropertyKey);
  }
}
