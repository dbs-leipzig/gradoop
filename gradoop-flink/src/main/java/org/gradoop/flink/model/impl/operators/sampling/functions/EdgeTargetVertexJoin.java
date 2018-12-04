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

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 * Joins to get the edge target
 */
public class EdgeTargetVertexJoin implements
  JoinFunction<Tuple3<Edge, GradoopId, Boolean>, Vertex, Tuple3<Edge, Boolean, Boolean>> {

  /**
   *  Reduce object instantiations
   */
  private Tuple3<Edge, Boolean, Boolean> reuse;

  /**
   * Property key of vertex value
   */
  private final String propertyKey;

  /**
   * Creates an instance of this join function
   *
   * @param propertyKey property key of marked vertex value
   */
  public EdgeTargetVertexJoin(String propertyKey) {
    this.reuse = new Tuple3<>();
    this.propertyKey = propertyKey;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Tuple3<Edge, Boolean, Boolean> join(Tuple3<Edge, GradoopId, Boolean> result,
                                           Vertex vertex) {
    reuse.f0 = result.f0;
    reuse.f1 = result.f2;
    reuse.f2 = vertex.getPropertyValue(propertyKey).getBoolean();
    return reuse;
  }
}
