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

package org.gradoop.examples.io.parsers.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;
import org.gradoop.examples.io.parsers.inputfilerepresentations.AdjacencyListable;
import org.gradoop.examples.io.parsers.inputfilerepresentations.Edgable;

/**
 * Maps each element from the adjacency list to a collection of edges
 * @param <K> comparable type
 * @param <X> Edge Type
 * @param <T> Adjacency List Type
 */
public class ToEdgesFromAdjList<K extends Comparable<K>, X extends Edgable<K>, T extends AdjacencyListable<K, X>> implements
  FlatMapFunction<T, ImportEdge<K>> {

  /**
   * Since this function is a flatMap using an adjacency list, then I could use the
   * ToEdge function in order not to replicate the code logic.
   */
  private ToEdge<K, X> underneathReusable;

  /**
   * Default construtcor
   * @param underneathReusable  Function mapping the edges
   */
  public ToEdgesFromAdjList(ToEdge<K, X> underneathReusable) {
    this.underneathReusable = underneathReusable;
  }

  @Override
  public void flatMap(T value, Collector<ImportEdge<K>> out) throws
    Exception {
    for (X x : value.asIterable()) {
      out.collect(underneathReusable.map(x));
    }
  }

}
