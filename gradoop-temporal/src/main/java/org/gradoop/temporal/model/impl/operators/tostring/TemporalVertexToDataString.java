/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
package org.gradoop.temporal.model.impl.operators.tostring;

import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.tostring.api.VertexToString;
import org.gradoop.flink.model.impl.operators.tostring.tuples.VertexString;
import org.gradoop.temporal.model.impl.operators.tostring.functions.TemporalElementToDataString;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;


/**
 * Represents a temporal vertex by a data string (label, properties and valid time).
 *
 * @param <V> temporal vertex type
 */
public class TemporalVertexToDataString<V extends TemporalVertex> extends TemporalElementToDataString<V>
  implements VertexToString<V> {

  @Override
  public void flatMap(V vertex, Collector<VertexString> collector) {
    GradoopId vertexId = vertex.getId();
    String vertexLabel = "(" + labelWithProperties(vertex) + time(vertex) + ")";

    for (GradoopId graphId : vertex.getGraphIds()) {
      collector.collect(new VertexString(graphId, vertexId, vertexLabel));
    }
  }
}

