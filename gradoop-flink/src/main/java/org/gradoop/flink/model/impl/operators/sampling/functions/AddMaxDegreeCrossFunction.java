/*
 * Copyright © 2014 - 2021 Leipzig University (Database Research Group)
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

import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.gradoop.common.model.api.entities.Vertex;

/**
 * A cross function that adds the max degree to all vertices.
 *
 * @param <V> The vertex type.
 */
public class AddMaxDegreeCrossFunction<V extends Vertex> implements CrossFunction<Tuple1<Long>, V, V> {

  /**
   * Name of the property for maximum degree
   */
  private String nameOfMaxDegreeProperty;

  /**
   * Creates an instance of this cross function.
   *
   * @param nameOfMaxDegreeProperty name of the property for maximum degree
   */
  public AddMaxDegreeCrossFunction(String nameOfMaxDegreeProperty) {
    this.nameOfMaxDegreeProperty = nameOfMaxDegreeProperty;
  }

  @Override
  public V cross(Tuple1<Long> longTuple1, V vertex) {
    vertex.setProperty(nameOfMaxDegreeProperty, longTuple1.f0);
    return vertex;
  }
}
