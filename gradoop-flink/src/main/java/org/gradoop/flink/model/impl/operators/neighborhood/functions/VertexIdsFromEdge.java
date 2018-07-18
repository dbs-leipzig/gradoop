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
package org.gradoop.flink.model.impl.operators.neighborhood.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;

/**
 * Returns a tuple which contains the source id and target id of an edge or a tuple which contains
 * the target id and the source id of the same edge.
 */
public class VertexIdsFromEdge
  implements MapFunction<Edge, Tuple2<GradoopId, GradoopId>> {

  /**
   * False, if tuple contains of source id and target id. True if tuple contains of target id and
   * source id.
   */
  private boolean switched;

  /**
   * Avoid object instantiation.
   */
  private Tuple2<GradoopId, GradoopId> reuseTuple = new Tuple2<>();

  /**
   * Constructor which initiates a mapping to tuple of source id and target id.
   */
  public VertexIdsFromEdge() {
    this(false);
  }

  /**
   * Valued constructor.
   *
   * @param switched false for tuple of source id and target id, true for vice versa
   */
  public VertexIdsFromEdge(boolean switched) {
    this.switched = switched;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Tuple2<GradoopId, GradoopId> map(Edge edge) throws Exception {
    if (switched) {
      reuseTuple.setFields(edge.getTargetId(), edge.getSourceId());
    } else {
      reuseTuple.setFields(edge.getSourceId(), edge.getTargetId());
    }
    return reuseTuple;
  }
}
