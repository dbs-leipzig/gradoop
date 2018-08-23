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
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 * Filters the edges with sampled vertices. If any vertices of the edge does not have any related
 * property for sampling, we consider that vertex as not sampled.
 */
public class EdgesWithSampledVerticesFilter
  implements FilterFunction<Tuple3<Edge, Vertex, Vertex>> {
  /**
   * Property name which shows if a vertex is sampled
   */
  private String propertyNameForSampled;
  /**
   * type of neighborhood
   */
  private Neighborhood neighborType;

  /**
   * Constructor
   *
   * @param propertyNameForSampled property name which shows if a vertex is sampled
   * @param neighborType type of neighborhood
   */
  public EdgesWithSampledVerticesFilter(String propertyNameForSampled, Neighborhood neighborType) {
    this.propertyNameForSampled = propertyNameForSampled;
    this.neighborType = neighborType;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean filter(Tuple3<Edge, Vertex, Vertex> t3) {
    boolean isSourceVertexMarked = false;
    boolean isTargetVertexMarked = false;
    if (t3.f1.hasProperty(propertyNameForSampled)) {
      isSourceVertexMarked = Boolean.getBoolean(
              t3.f1.getPropertyValue(propertyNameForSampled).toString());
    }
    if (t3.f2.hasProperty(propertyNameForSampled)) {
      isTargetVertexMarked = Boolean.getBoolean(
              t3.f2.getPropertyValue(propertyNameForSampled).toString());
    }
    boolean ret = false;
    if (neighborType.equals(Neighborhood.BOTH)) {
      ret = isSourceVertexMarked || isTargetVertexMarked;
    } else if (neighborType.equals(Neighborhood.IN)) {
      ret = isTargetVertexMarked;
    } else if (neighborType.equals(Neighborhood.OUT)) {
      ret = isSourceVertexMarked;
    }
    return ret;
  }
}
