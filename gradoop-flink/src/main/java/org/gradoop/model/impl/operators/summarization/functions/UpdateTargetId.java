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

package org.gradoop.model.impl.operators.summarization.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.model.impl.operators.summarization.tuples.EdgeGroupItem;
import org.gradoop.model.impl.operators.summarization.tuples.VertexWithRepresentative;

/**
 * Takes a projected edge and an (vertex-id, group-representative) tuple
 * and replaces the edge-target-id with the group-representative.
 */
@FunctionAnnotation.ForwardedFieldsFirst("f0;f1;f3;f4")
@FunctionAnnotation.ForwardedFieldsSecond("f1->f2") // vertex id -> target id
public class UpdateTargetId implements
  JoinFunction<EdgeGroupItem, VertexWithRepresentative, EdgeGroupItem> {

  /**
   * {@inheritDoc}
   */
  @Override
  public EdgeGroupItem join(EdgeGroupItem edge,
    VertexWithRepresentative vertexRepresentative) throws Exception {
    edge.setField(vertexRepresentative.getGroupRepresentativeVertexId(), 2);
    return edge;
  }
}
