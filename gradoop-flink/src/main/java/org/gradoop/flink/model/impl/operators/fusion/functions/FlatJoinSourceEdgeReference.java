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
package org.gradoop.flink.model.impl.operators.fusion.functions;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 * Function updating the edges' sources or destination
 * for each newly created hypervertex
 */
public class FlatJoinSourceEdgeReference
  implements FlatJoinFunction<Edge, Tuple2<Vertex, GradoopId>, Edge> {

  /**
   * Checking if the stuff is actually updating the sources.
   * Otherwise, it updates the targets
   */
  private final boolean isItSourceDoingNow;

  /**
   * Default constructor
   * @param isItSourceDoingNow  If true, it does test the vertices. The targets are tested,
   *                            otherwise
   */
  public FlatJoinSourceEdgeReference(boolean isItSourceDoingNow) {
    this.isItSourceDoingNow = isItSourceDoingNow;
  }

  @Override
  public void join(Edge first, Tuple2<Vertex, GradoopId> second, Collector<Edge> out)
      throws Exception {
    if (second != null && !(second.f1.equals(GradoopId.NULL_VALUE))) {
      if (isItSourceDoingNow) {
        first.setSourceId(second.f1);
      } else {
        first.setTargetId(second.f1);
      }
    }
    out.collect(first);
  }

}
