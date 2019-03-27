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

import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.java.tuple.Tuple1;

/**
 * Calculates the degree centrality of the graph
 */
public class CalculateDegreeCentrality implements CrossFunction<Tuple1<Long>, Long, Double> {

  /**
   * Calculates the degree centrality of the graph
   * using the function: (Sum(d(max) - d(i)) / (v_count -2) * (v_count-1)
   *
   * @param vertexDistanceSum sum of degree distances of the vertices
   * @param vertexCount number of vertices
   * @return degree centrality of graph
   */
  @Override
  public Double cross(Tuple1<Long> vertexDistanceSum, Long vertexCount) {
    double sum = vertexDistanceSum.f0;
    return sum / ((vertexCount - 2) * (vertexCount - 1));
  }
}
