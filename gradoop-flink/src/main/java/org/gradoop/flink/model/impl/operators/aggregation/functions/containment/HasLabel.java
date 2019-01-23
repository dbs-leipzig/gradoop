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

import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.functions.AggregateFunction;
import org.gradoop.flink.model.impl.functions.filters.CombinableFilter;
import org.gradoop.flink.model.impl.operators.aggregation.functions.BaseAggregateFunction;
import org.gradoop.flink.model.impl.operators.aggregation.functions.bool.Or;

import java.util.Objects;

/**
 * Superclass of aggregate and filter functions that check vertex or edge label
 * presence in a graph.
 *
 * <pre>
 * Usage:
 * <ol>
 * <li>aggregate
 * <li>filter using the same UDF instance.
 * </ol>
 * </pre>
 */
public class HasLabel extends BaseAggregateFunction
  implements Or, AggregateFunction, CombinableFilter<GraphHead> {

  /**
   * Label to check presence of.
   */
  private final String label;

  /**
   * Creates a new instance of a HasLabel aggregate function.
   *
   * @param label element label to check presence of
   */
  public HasLabel(String label) {
    this(label, "hasLabel_" + label);
  }

  /**
   * Creates a new instance of a HasLabel aggregate function.
   *
   * @param label label to check presence of
   * @param aggregatePropertyKey aggregate property key
   */
  public HasLabel(String label, String aggregatePropertyKey) {
    super(aggregatePropertyKey);
    Objects.requireNonNull(label);
    this.label = label;
  }

  @Override
  public PropertyValue getIncrement(Element element) {
    return PropertyValue.create(element.getLabel().equals(label));
  }

  @Override
  public boolean filter(GraphHead graphHead) throws Exception {
    return graphHead.getPropertyValue(getAggregatePropertyKey()).getBoolean();
  }
}
