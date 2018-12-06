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
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 * Joins to get the edge source:
 * (edge),(vertex) -> (edge,edge.targetId,(bool)vertex[propertyKey])
 */
@FunctionAnnotation.ForwardedFieldsFirst({"*->f0", "id->f1"})
@FunctionAnnotation.ReadFieldsSecond("properties")
public class EdgeSourceVertexJoin
  implements JoinFunction<Edge, Vertex, Tuple3<Edge, GradoopId, Boolean>> {
  /**
   * Reduce object instantiations
   */
  private Tuple3<Edge, GradoopId, Boolean> reuse;

  /**
   * Property key of marked value
   */
  private String propertyKey;

  /**
   * Creates an instance of this join function
   *
   * @param propertyKey vertex property key
   */
  public EdgeSourceVertexJoin(String propertyKey) {
    this.reuse = new Tuple3<>();
    this.propertyKey = propertyKey;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Tuple3<Edge, GradoopId, Boolean> join(Edge edge, Vertex vertex) throws Exception {
    reuse.f0 = edge;
    reuse.f1 = edge.getTargetId();
    reuse.f2 = vertex.getPropertyValue(propertyKey).getBoolean();
    return reuse;
  }
}
