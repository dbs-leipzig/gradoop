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
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.tostring.api.EdgeToString;
import org.gradoop.flink.model.impl.operators.tostring.tuples.EdgeString;

/**
 * represents an edge by a data string (label and properties)
 */
public class EdgeToDataString extends ElementToDataString<Edge> implements
  EdgeToString<Edge> {

  @Override
  public void flatMap(Edge edge, Collector<EdgeString> collector)
      throws Exception {

    GradoopId sourceId = edge.getSourceId();
    GradoopId targetId = edge.getTargetId();
    String edgeLabel = "[" + label(edge) + "]";

    for (GradoopId graphId : edge.getGraphIds()) {
      collector.collect(new EdgeString(graphId, sourceId, targetId, edgeLabel));
    }
  }
}
