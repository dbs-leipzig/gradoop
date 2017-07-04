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
package org.gradoop.flink.model.impl.operators.split.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdList;

/**
 * Adds new graph ids to the initial vertex set
 *
 * @param <V> EPGM vertex type
 */
@FunctionAnnotation.ReadFieldsFirst("graphIds")
@FunctionAnnotation.ReadFieldsSecond("f1")
public class AddNewGraphsToVertex<V extends Vertex>
  implements JoinFunction<V, Tuple2<GradoopId, GradoopIdList>, V> {
  /**
   * {@inheritDoc}
   */
  @Override
  public V join(V vertex,
    Tuple2<GradoopId, GradoopIdList> vertexWithGraphIds) {
    vertex.getGraphIds().addAll(vertexWithGraphIds.f1);
    return vertex;
  }

}
