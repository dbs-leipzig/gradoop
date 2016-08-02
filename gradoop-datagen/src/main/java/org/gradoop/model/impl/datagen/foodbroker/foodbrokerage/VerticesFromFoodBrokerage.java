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

package org.gradoop.model.impl.datagen.foodbroker.foodbrokerage;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMVertex;

import java.util.Set;

/**
 * Flat map which returns all vertices created by the foodbrokerage process.
 *
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class VerticesFromFoodBrokerage<V extends EPGMVertex, E extends EPGMEdge>
  implements FlatMapFunction<Tuple2<Set<V>, Set<E>>, V> {

  @Override
  public void flatMap(
    Tuple2<Set<V>, Set<E>> tuple, Collector<V> collector) throws Exception {
    for (V vertex : tuple.f0) {
      collector.collect(vertex);
    }
  }
}
