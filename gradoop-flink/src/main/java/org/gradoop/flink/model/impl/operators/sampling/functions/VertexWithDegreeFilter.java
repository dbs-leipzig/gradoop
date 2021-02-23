/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.operators.sampling.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.common.model.api.entities.Vertex;

/**
 * Retains all vertices which do not have the given degree.
 * If a vertex does not have the degree property, it will be filtered.
 *
 * @param <V> Vertex type with property for degree ("deg")
 */
public class VertexWithDegreeFilter<V extends Vertex> implements FilterFunction<V> {
  /**
   * Given degree to be filtered
   */
  private final long degree;
  /**
   * Name of property of degree
   */
  private final String degreePropertyName;
  /**
   * Constructor
   *
   * @param degree the given degree
   * @param degreePropertyName the name of property of degree
   */
  public VertexWithDegreeFilter(long degree, String degreePropertyName) {
    this.degree = degree;
    this.degreePropertyName = degreePropertyName;
  }

  @Override
  public boolean filter(V vertex) throws Exception {
    if (vertex.hasProperty(degreePropertyName)) {
      return Long.parseLong(vertex.getPropertyValue(degreePropertyName).toString()) != degree;
    } else {
      return true;
    }
  }
}
