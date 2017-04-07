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

package org.gradoop.benchmark.nesting.parsers;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;

/**
 * Converts an element A into an element B-indexed that will become an edge
 * @param <A> element to be converted into an edge
 * @param <B> index associated to A
 */
public interface ConvertStringToEdge<A, B extends Comparable<B>>
  extends MapFunction<A,  ImportEdge<B>>, FlatMapFunction<A, ImportEdge<B>> {

  /**
   * Checks if the argument is valid to be collected
   * @param toCheck Element to be check'd
   * @return        Validity
   */
  boolean isValid(A toCheck);

  @Override
  default void flatMap(A x, Collector<ImportEdge<B>> coll) throws Exception {
    if (isValid(x)) {
      coll.collect(map(x));
    }
  }
}
