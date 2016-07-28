/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.model.impl.operators.grouping.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.gradoop.model.impl.operators.grouping.tuples.VertexWithSuperVertex;
import org.gradoop.model.impl.operators.grouping.tuples.EdgeGroupItem;

/**
 * Takes a projected edge and an (vertex-id, group-representative) tuple
 * and replaces the edge-target-id with the group-representative.
 */
public class UpdateEdgeGroupItem implements
  JoinFunction<EdgeGroupItem, VertexWithSuperVertex, EdgeGroupItem> {

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
  public EdgeGroupItem join(EdgeGroupItem edge, VertexWithSuperVertex idTuple)
      throws Exception {
    edge.setField(idTuple.getSuperVertexId(), field);
    return edge;
  }
}
