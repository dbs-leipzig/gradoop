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

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.Edge;

/**
 * Filters the edges with sampled vertices. If any vertices of the edge does not have any related
 * property for sampling, we consider that vertex as not sampled.
 */
public class EdgesWithSampledVerticesFilter
  implements FilterFunction<Tuple3<Edge, Boolean, Boolean>> {

  /**
   * Type of neighborhood
   */
  private Neighborhood neighborType;

  /**
   * Constructor
   *
   * @param neighborType type of neighborhood
   */
  public EdgesWithSampledVerticesFilter(Neighborhood neighborType) {
    this.neighborType = neighborType;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean filter(Tuple3<Edge, Boolean, Boolean> tuple) {

    boolean isSourceVertexMarked = tuple.f1;
    boolean isTargetVertexMarked = tuple.f2;

    boolean filter = false;

    if (neighborType.equals(Neighborhood.BOTH)) {
      filter = isSourceVertexMarked && isTargetVertexMarked;
    } else if (neighborType.equals(Neighborhood.IN)) {
      filter = isTargetVertexMarked;
    } else if (neighborType.equals(Neighborhood.OUT)) {
      filter = isSourceVertexMarked;
    }

    return filter;
  }
}
