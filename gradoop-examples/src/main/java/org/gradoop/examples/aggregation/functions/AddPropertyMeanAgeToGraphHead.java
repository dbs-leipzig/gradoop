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
package org.gradoop.examples.aggregation.functions;

import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.flink.model.api.functions.TransformationFunction;

/**
 * Custom graph head transformation function used in {@link org.gradoop.examples.aggregation.AggregationExample}
 */
public class AddPropertyMeanAgeToGraphHead implements TransformationFunction<GraphHead> {

  /**
   * Property key 'mean_age'
   */
  private static final String PROPERTY_KEY_MEAN_AGE = "mean_age";

  @Override
  public GraphHead apply(GraphHead current, GraphHead transformed) {
    // property 'sum_birthday' is result of the aggregate function SumVertexProperty
    int sumAge = current.getPropertyValue("sum_birthday").getInt();
    // property 'vertexCount' is result of the aggregate function VertexCount
    long numPerson = current.getPropertyValue("vertexCount").getLong();
    current.setProperty(PROPERTY_KEY_MEAN_AGE, (double) sumAge / numPerson);
    return current;
  }
}
