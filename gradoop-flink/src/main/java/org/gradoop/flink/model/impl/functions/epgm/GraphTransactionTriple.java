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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.representation.transactional.GraphTransaction;
import org.gradoop.common.model.impl.pojo.GraphHead;

import java.util.Set;

/**
 * graphTransaction <=> (graphHead, {vertex,..}, {edge, ..})
 *
 * Forwarded fields:
 *
 * f0: transaction graph head
 * f1: transaction vertices
 * f2: transaction edges
 */
@FunctionAnnotation.ForwardedFields("f0;f1;f2")
public class GraphTransactionTriple
  implements MapFunction<GraphTransaction, Tuple3<GraphHead, Set<Vertex>, Set<Edge>>> {
  /**
   * Reduce object instantiations
   */
  private final Tuple3<GraphHead, Set<Vertex>, Set<Edge>> reuseTuple = new Tuple3<>();

  @Override
  public Tuple3<GraphHead, Set<Vertex>, Set<Edge>> map(GraphTransaction transaction)
    throws Exception {

    reuseTuple.f0 = transaction.getGraphHead();
    reuseTuple.f1 = transaction.getVertices();
    reuseTuple.f2 = transaction.getEdges();

    return reuseTuple;
  }
}
