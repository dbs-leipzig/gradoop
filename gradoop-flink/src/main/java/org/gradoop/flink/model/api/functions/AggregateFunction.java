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
package org.gradoop.flink.model.api.functions;

import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.aggregation.Aggregation;

import java.io.Serializable;

/**
 * Describes an aggregate function as input for the {@link Aggregation} operator.
 */
public interface AggregateFunction extends Serializable {

  /**
   * Describes the aggregation logic.
   *
   * @param aggregate previously aggregated value
   * @param increment value that is added to the aggregate
   *
   * @return new aggregate
   */
  PropertyValue aggregate(PropertyValue aggregate, PropertyValue increment);

  /**
   * Returns the property key used to store the aggregate value.
   *
   * @return aggregate property key
   */
  String getAggregatePropertyKey();

  /**
   * Describes the increment of an element that should be added to the aggregate.
   *
   * @param element element used to get the increment
   * @return increment, may be NULL, which is handled in the operator
   */
  PropertyValue getIncrement(Element element);

  /**
   * Returns whether this function aggregates vertices.
   *
   * @return true, if it aggregates vertices
   */
  default boolean isVertexAggregation() {
    return true;
  }
  /**
   * Returns whether this function aggregates edges.
   *
   * @return true, if it aggregates edges
   */
  default boolean isEdgeAggregation() {
    return true;
  }
}
