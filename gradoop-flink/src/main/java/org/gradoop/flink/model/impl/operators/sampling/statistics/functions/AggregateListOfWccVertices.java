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
package org.gradoop.flink.model.impl.operators.sampling.statistics.functions;

import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.functions.VertexAggregateFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * Aggregates the connected component ids from all vertices as list of propertyValues.
 */
public class AggregateListOfWccVertices implements VertexAggregateFunction {

  /**
   * Property key to retrieve property values
   */
  private final String wccPropertyKey;

  /**
   * Property key for aggregated list of property values
   */
  private final String listOfWccIDsPropertyKey;

  /**
   * Creates a new instance of a AggregateListOfWccVertices aggregate function.
   *
   * @param wccPropertyKey Property key to retrieve property values
   */
  public AggregateListOfWccVertices(String wccPropertyKey) {
    this.wccPropertyKey = wccPropertyKey;
    this.listOfWccIDsPropertyKey = "vertices_" + this.wccPropertyKey;
  }

  @Override
  public PropertyValue getIncrement(Element vertex) {
    List<PropertyValue> valueList = new ArrayList<>();
    valueList.add(PropertyValue.create(vertex.getPropertyValue(wccPropertyKey).toString()));
    return PropertyValue.create(valueList);
  }

  @Override
  public PropertyValue aggregate(PropertyValue aggregate, PropertyValue increment) {
    List<PropertyValue> aggregateList = aggregate.getList();
    aggregateList.addAll(increment.getList());
    aggregate.setList(aggregateList);
    return aggregate;
  }

  @Override
  public String getAggregatePropertyKey() {
    return listOfWccIDsPropertyKey;
  }
}
