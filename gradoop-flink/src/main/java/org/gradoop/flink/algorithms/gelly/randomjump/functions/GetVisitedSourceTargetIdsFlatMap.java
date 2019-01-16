/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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
package org.gradoop.flink.algorithms.gelly.randomjump.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;

/**
 * Retrieves the source- and target-id from an edge, if this edge was visited, determined by the
 * boolean property.
 */
@FunctionAnnotation.ReadFields("properties")
public class GetVisitedSourceTargetIdsFlatMap implements FlatMapFunction<Edge, GradoopId> {

  /**
   * Key for the boolean property value to determine, if the edge was visited.
   */
  private final String propertyKey;

  /**
   * Creates an instance of GetVisitedSourceTargetIdsFlatMap with a given property key.
   *
   * @param propertyKey Key for the boolean property value to determine, if the edge was visited.
   */
  public GetVisitedSourceTargetIdsFlatMap(String propertyKey) {
    this.propertyKey = propertyKey;
  }

  @Override
  public void flatMap(Edge edge, Collector<GradoopId> out) throws Exception {
    if (edge.getPropertyValue(propertyKey).getBoolean()) {
      out.collect(edge.getSourceId());
      out.collect(edge.getTargetId());
    }
  }
}
