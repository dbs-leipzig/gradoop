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
package org.gradoop.flink.model.impl.operators.statistics.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.properties.PropertyValue;

/**
 * Reads the aggregated values for the sum of all local clustering coefficients and the vertex
 * count, computes the average value and writes it as property to the graph head.
 * Checks for and catches cases where {@code sum.isNull()} or {@code vertexCount == 0L} to avoid
 * {@code Double.NaN} as result.
 */
public class AddAverageCCValueToGraphHeadMap implements MapFunction<GraphHead, GraphHead> {

  /**
   * Property key to access the sum of all local clustering coefficients
   */
  private final String propertyKeySumLocal;

  /**
   * Property key to access the vertex count
   */
  private final String propertyKeyVertexCount;

  /**
   * Property key to store the average value
   */
  private final String propertyKeyAverage;

  /**
   * Creates an instance of AddAverageCCValueToGraphHeadMap.
   *
   * @param propertyKeySumLocal Property key to access the sum of all local clustering coefficients
   * @param propertyKeyVertexCount Property key to access the vertex count
   * @param propertyKeyAverage Property key to store the average value
   */
  public AddAverageCCValueToGraphHeadMap(String propertyKeySumLocal, String propertyKeyVertexCount,
    String propertyKeyAverage) {
    this.propertyKeySumLocal = propertyKeySumLocal;
    this.propertyKeyVertexCount = propertyKeyVertexCount;
    this.propertyKeyAverage = propertyKeyAverage;
  }

  @Override
  public GraphHead map(GraphHead graphHead) {
    PropertyValue averageProperty = graphHead.getPropertyValue(propertyKeySumLocal);
    double sumLocal = averageProperty.isNull() ? 0.0 : averageProperty.getDouble();
    long vertexCount = graphHead.getPropertyValue(propertyKeyVertexCount).getLong();
    graphHead.setProperty(propertyKeyAverage, vertexCount == 0L ? 0.0 :
      (sumLocal / (double) vertexCount));
    return graphHead;
  }
}
