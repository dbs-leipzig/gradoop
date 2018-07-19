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
package org.gradoop.flink.model.impl.operators.sampling.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 * A map from a vertex to its degree
 */
public class VertexToDegreeMap implements MapFunction<Vertex, Tuple1<Long>> {
  /**
   * The property name for degree
   */
  private String nameOfDegreeProperty;

  /**
   * Constructor
   *
   * @param nameOfDegreeProperty the property name for degree
   */
  public VertexToDegreeMap(String nameOfDegreeProperty) {
    this.nameOfDegreeProperty = nameOfDegreeProperty;
  }

  /**
   * @param vertex the given vertex
   * @return the degree of that vertex
   */
  @Override
  public Tuple1<Long> map(Vertex vertex) {
    return new Tuple1<>(Long.parseLong(vertex.getPropertyValue(nameOfDegreeProperty).toString()));
  }
}
