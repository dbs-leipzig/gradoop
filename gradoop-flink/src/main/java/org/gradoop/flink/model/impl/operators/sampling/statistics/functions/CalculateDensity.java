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
package org.gradoop.flink.model.impl.operators.sampling.statistics.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.pojo.GraphHead;

/**
 * Calculates the graph density and safes the value as property to the corresponding graphHead.
 */
public class CalculateDensity implements MapFunction<GraphHead, GraphHead> {

  /**
   * Used property key
   */
  private String propertyKey;

  /**
   * Public constructor
   *
   * @param key used property key
   */
  public CalculateDensity(String key) {
    this.propertyKey = key;
  }

  /**
   * Calculates the graph density and safes the value as property to the graphHead.
   *
   * @param graphHead The graphHead the density shall be written to
   * @return GraphHead The graphHead the density is written to
   */
  @Override
  public GraphHead map(GraphHead graphHead) {
    double vc1 = (double) graphHead.getPropertyValue("vertexCount").getLong();
    double ec1 = (double) graphHead.getPropertyValue("edgeCount").getLong();
    double density = ec1 / (vc1 * (vc1 - 1.));
    graphHead.setProperty(propertyKey, density);
    return graphHead;
  }
}
