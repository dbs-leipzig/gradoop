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
package org.gradoop.flink.model.impl.operators.grouping.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.gradoop.flink.model.impl.operators.grouping.tuples.EdgeGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.tuples.VertexWithSuperVertex;

/**
 * Takes a projected edge and an (vertex-id, group-representative) tuple
 * and replaces the edge-target-id with the group-representative.
 */
public class UpdateEdgeGroupItem
  implements JoinFunction<EdgeGroupItem, VertexWithSuperVertex, EdgeGroupItem> {
  /**
   * Field in {@link EdgeGroupItem} which is overridden by the group
   * representative id.
   */
  private final int field;
  /**
   * Creates new join function.
   *
   * @param field field that is overridden by the group representative
   */
  public UpdateEdgeGroupItem(int field) {
    this.field = field;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public EdgeGroupItem join(EdgeGroupItem edge, VertexWithSuperVertex idTuple) throws Exception {
    edge.setField(idTuple.getSuperVertexId(), field);
    return edge;
  }
}
