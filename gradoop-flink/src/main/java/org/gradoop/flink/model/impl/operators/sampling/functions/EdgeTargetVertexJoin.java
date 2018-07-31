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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 * Joins to get the edge target
 */
public class EdgeTargetVertexJoin implements JoinFunction<Tuple3<Edge, Vertex, GradoopId>,
        Tuple2<Vertex, GradoopId>, Tuple3<Edge, Vertex, Vertex>> {
  /**
   *  Reduce object instantiations
   */
  private Tuple3<Edge, Vertex, Vertex> reuse;

  /**
   * Constructor
   */
  public EdgeTargetVertexJoin() {
    reuse = new Tuple3<>();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Tuple3<Edge, Vertex, Vertex> join(Tuple3<Edge, Vertex, GradoopId> edgeWithItsVerticesIds,
                                           Tuple2<Vertex, GradoopId> vertexWithItsId) {
    reuse.f0 = edgeWithItsVerticesIds.f0;
    reuse.f1 = edgeWithItsVerticesIds.f1;
    reuse.f2 = vertexWithItsId.f0;
    return reuse;
  }
}
