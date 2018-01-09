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
package org.gradoop.flink.model.impl.operators.grouping.functions.vertexcentric;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.flink.model.impl.operators.grouping.tuples.VertexWithSuperVertex;
import org.gradoop.flink.model.impl.operators.grouping.tuples.vertexcentric.VertexGroupItem;

/**
 * Maps a {@link VertexGroupItem} to a {@link VertexWithSuperVertex}.
 */
@FunctionAnnotation.ForwardedFields(
  "f0;" + // vertex id
  "f1"    // super vertex id
)
public class BuildVertexWithSuperVertex
  implements MapFunction<VertexGroupItem, VertexWithSuperVertex> {

  /**
   * Avoid object instantiation.
   */
  private final VertexWithSuperVertex reuseTuple;

  /**
   * Creates mapper.
   */
  public BuildVertexWithSuperVertex() {
    this.reuseTuple = new VertexWithSuperVertex();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public VertexWithSuperVertex map(VertexGroupItem vertexGroupItem) throws
    Exception {
    reuseTuple.setVertexId(vertexGroupItem.getVertexId());
    reuseTuple.setSuperVertexId(vertexGroupItem.getSuperVertexId());
    return reuseTuple;
  }
}
