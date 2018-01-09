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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.grouping.tuples.edgecentric.SuperVertexGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.tuples.VertexWithSuperVertex;

/**
 * Maps the related ids for each vertex which is part of a super vertex .
 */
@FunctionAnnotation.ForwardedFields("f1")
@FunctionAnnotation.ReadFields("f0")
public class BuildVertexWithSuperVertexFromItem
  implements FlatMapFunction<SuperVertexGroupItem, VertexWithSuperVertex> {

  /**
   * Avoid object initialization in each call.
   */
  private VertexWithSuperVertex vertexWithSuperVertex = new VertexWithSuperVertex();

  /**
   * {@inheritDoc}
   */
  @Override
  public void flatMap(SuperVertexGroupItem superVertexGroupItem,
    Collector<VertexWithSuperVertex> collector) throws Exception {

    vertexWithSuperVertex.setSuperVertexId(superVertexGroupItem.getSuperVertexId());
    for (GradoopId gradoopId : superVertexGroupItem.getVertexIds()) {
      vertexWithSuperVertex.setVertexId(gradoopId);
      collector.collect(vertexWithSuperVertex);
    }
  }
}
