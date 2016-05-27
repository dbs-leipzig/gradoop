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

package org.gradoop.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.tuples.GraphTransaction;

import java.util.Set;

/**
 * graphTransaction <=> (graphHead, {vertex,..}, {edge, ..})
 *
 * @param <G> graph head type
 * @param <V> vertex type
 * @param <E> edge type
 */
public class GraphTransactionTriple
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements MapFunction<GraphTransaction<G, V, E>, Tuple3<G, Set<V>, Set<E>>>,
  JoinFunction<Tuple3<GradoopId, Set<V>, Set<E>>, G, GraphTransaction<G, V, E>>
{
  @Override
  public Tuple3<G, Set<V>, Set<E>> map(
    GraphTransaction<G, V, E> transaction) throws Exception {

    return new Tuple3<>(transaction.f0, transaction.f1, transaction.f2);
  }

  @Override
  public GraphTransaction<G, V, E> join(
    Tuple3<GradoopId, Set<V>, Set<E>> triple, G graphHead) throws Exception {

    return new GraphTransaction<>(graphHead, triple.f1, triple.f2);
  }
}
