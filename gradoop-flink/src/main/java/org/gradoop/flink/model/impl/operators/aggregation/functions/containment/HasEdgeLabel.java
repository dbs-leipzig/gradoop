/**
 * Copyright © 2014 - 2017 Leipzig University (Database Research Group)
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

import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.functions.EdgeAggregateFunction;

/**
 * aggregate and filter function to presence of an edge label in a graph.
 *
 * Usage: First, aggregate and, second, filter using the same UDF instance.
 */
public class HasEdgeLabel extends HasLabel implements EdgeAggregateFunction {

  /**
   * Constructor.
   *
   * @param label vertex label to check presence of
   */
  public HasEdgeLabel(String label) {
    super(label);
  }

  @Override
  public PropertyValue getEdgeIncrement(Edge edge) {
    return PropertyValue.create(edge.getLabel().equals(label));
  }

  @Override
  public String getAggregatePropertyKey() {
    return "hasEdgeLabel_" + label;
  }
}
