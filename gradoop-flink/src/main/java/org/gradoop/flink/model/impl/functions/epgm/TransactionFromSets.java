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

package org.gradoop.flink.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.representation.transaction.GraphTransaction;

import java.util.HashSet;
import java.util.Set;

/**
 * (graphHead) =|><| (graphId,{vertex,..},{edge,..}) => (graphHead,{vertex,..},{edge,..})
 *
 * Forwarded fields first:
 *
 * f0: graph head
 *
 * Forwarded fields second:
 *
 * f1: vertex set
 * f2: edge set
 */
@FunctionAnnotation.ForwardedFieldsFirst("*->f0")
@FunctionAnnotation.ForwardedFieldsSecond("f1;f2")
public class TransactionFromSets
  implements JoinFunction<GraphHead, Tuple3<GradoopId, Set<Vertex>, Set<Edge>>, GraphTransaction> {
  /**
   * Used if right hand side is empty
   */
  private static final Set<Vertex> EMPTY_VERTEX_SET = new HashSet<>(0);
  /**
   * Used if right hand side is empty
   */
  private static final Set<Edge> EMPTY_EDGE_SET = new HashSet<>(0);
  /**
   * Reduce object instantiations
   */
  private final GraphTransaction reuseTransaction = new GraphTransaction();

  @Override
  public GraphTransaction join(GraphHead graphHead, Tuple3<GradoopId, Set<Vertex>, Set<Edge>> sets)
    throws Exception {

    reuseTransaction.setGraphHead(graphHead);
    reuseTransaction.setVertices(sets == null ? EMPTY_VERTEX_SET : sets.f1);
    reuseTransaction.setEdges(sets == null ? EMPTY_EDGE_SET : sets.f2);

    return reuseTransaction;
  }
}
