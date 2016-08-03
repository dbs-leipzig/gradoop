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
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.tuples.GraphTransaction;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.id.GradoopId;

import java.util.Set;

/**
 * graphTransaction <=> (graphHead, {vertex,..}, {edge, ..})
 */
public class GraphTransactionTriple implements
  MapFunction<GraphTransaction, Tuple3<GraphHead, Set<Vertex>, Set<Edge>>>,
  JoinFunction
    <Tuple3<GradoopId, Set<Vertex>, Set<Edge>>, GraphHead, GraphTransaction> {

  @Override
  public Tuple3<GraphHead, Set<Vertex>, Set<Edge>> map(
    GraphTransaction transaction) throws Exception {

    return new Tuple3<>(transaction.f0, transaction.f1, transaction.f2);
  }

  @Override
  public GraphTransaction join(Tuple3<GradoopId, Set<Vertex>, Set<Edge>> triple,
    GraphHead graphHead) throws Exception {

    return new GraphTransaction(graphHead, triple.f1, triple.f2);
  }
}
