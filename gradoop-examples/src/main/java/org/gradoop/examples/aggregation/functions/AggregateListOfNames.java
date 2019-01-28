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
package org.gradoop.examples.aggregation.functions;

import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.functions.VertexAggregateFunction;
import org.gradoop.flink.model.impl.operators.aggregation.functions.BaseAggregateFunction;

/**
 * Custom vertex aggregate function that stores all values of the property 'name' over a set of
 * vertices as a comma separated list in a property named 'list_of_names'.
 * Used in {@link org.gradoop.examples.aggregation.AggregationExample}
 */
public class AggregateListOfNames extends BaseAggregateFunction implements VertexAggregateFunction {

  /**
   * Property key 'name'
   */
  private static final String PROPERTY_KEY_NAME = "name";

  /**
   * Creates a new instance of a AggregateListOfNames aggregate function.
   */
  public AggregateListOfNames() {
    super("list_of_names");
  }

  @Override
  public PropertyValue getIncrement(Element vertex) {
    return PropertyValue.create(vertex.getPropertyValue(PROPERTY_KEY_NAME).toString());
  }

  @Override
  public PropertyValue aggregate(PropertyValue aggregate, PropertyValue increment) {
    aggregate.setString(aggregate.getString() + "," + increment.getString());
    return aggregate;
  }
}
