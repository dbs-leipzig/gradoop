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
package org.gradoop.flink.model.impl.operators.tostring.functions;

import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.operators.tostring.api.VertexToString;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.tostring.tuples.VertexString;

/**
 * represents a vertex by a data string (label and properties)
 */
public class VertexToDataString
  extends ElementToDataString<Vertex>
  implements VertexToString<Vertex> {

  @Override
  public void flatMap(Vertex vertex, Collector<VertexString> collector) throws Exception {
    GradoopId vertexId = vertex.getId();
    String vertexLabel = "(" + label(vertex) + ")";

    for (GradoopId graphId : vertex.getGraphIds()) {
      collector.collect(new VertexString(graphId, vertexId, vertexLabel));
    }
  }
}
