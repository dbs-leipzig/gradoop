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
package org.gradoop.flink.model.impl.operators.fusion.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.api.entities.GraphElement;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * A variant of the AddToGraph, where we have no graph head
 * but a graph id
 * @param <K> element receiving the new graph id
 *
 */
public class MapFunctionAddGraphElementToGraph2<K extends GraphElement> implements MapFunction<K, K> {

  /**
   * Graph Id that has to be added
   */
  private final GradoopId newGraphId;

  /**
   * Default constructor
   * @param newGraphId  Graph Id that has to be added
   */
  public MapFunctionAddGraphElementToGraph2(GradoopId newGraphId) {
    this.newGraphId = newGraphId;
  }

  @Override
  public K map(K value) throws Exception {
    value.addGraphId(newGraphId);
    return value;
  }
}
