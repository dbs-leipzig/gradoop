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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;

import java.util.Set;

/**
 * (graphHead, {vertex,..}, {edge,..}) => edge,..
 *
 * @param <G> graph head type
 * @param <V> vertex type
 * @param <E> edge type
 */
public class TransactionEdges
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements FlatMapFunction<Tuple3<G, Set<V>, Set<E>>, E> {

  @Override
  public void flatMap(Tuple3<G, Set<V>, Set<E>> graphTriple,
    Collector<E> collector) throws Exception {

    for (E edge : graphTriple.f2) {
      collector.collect(edge);
    }
  }
}
