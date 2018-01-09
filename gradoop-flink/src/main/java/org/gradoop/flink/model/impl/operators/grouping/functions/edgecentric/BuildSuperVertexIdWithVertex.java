/**
 * Copyright © 2014 - 2017 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.operators.grouping.functions.edgecentric;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.operators.grouping.tuples.edgecentric.SuperVertexIdWithVertex;
import org.gradoop.flink.model.impl.operators.grouping.tuples.VertexWithSuperVertex;

/**
 * Assigns the graphs vertices to all super vertex ids they are part of.
 */
@FunctionAnnotation.ForwardedFieldsFirst("f1->f0")
public class BuildSuperVertexIdWithVertex
  implements JoinFunction<VertexWithSuperVertex, Vertex, SuperVertexIdWithVertex> {

  /**
   * Avoid object initialization in each call.
   */
  private SuperVertexIdWithVertex superVertexIdWithVertex = new SuperVertexIdWithVertex();

  /**
   * {@inheritDoc}
   */
  @Override
  public SuperVertexIdWithVertex join(VertexWithSuperVertex vertexWithSuperVertex, Vertex vertex)
    throws Exception {
    superVertexIdWithVertex.setSuperVertexid(vertexWithSuperVertex.getSuperVertexId());
    superVertexIdWithVertex.setVertex(vertex);
    return superVertexIdWithVertex;
  }
}
