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

package org.gradoop.flink.io.reader.parsers.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.flink.io.reader.parsers.inputfilerepresentations.AdjacencyListable;
import org.gradoop.flink.io.reader.parsers.inputfilerepresentations.Edgable;

/**
 * Converts an object that could be represented as a vertex (Vertexable) into a
 * proper vertex instance.
 *
 * @param <K> Comparable Type
 * @param <X> Edge Type
 * @param <T> Adjacency List Type
 */
public class FromStringToAdjacencyList<K extends Comparable<K>, X extends Edgable<K>, T extends AdjacencyListable<K, X>>
  implements
  MapFunction<String, T> {

  /**
   * Reusable element
   */
  private final T reusableToConvert;

  /**
   * Default constructor
   * @param reusableToConvert element to be passed that is updated
   */
  public FromStringToAdjacencyList(T reusableToConvert) {
    this.reusableToConvert = reusableToConvert;
  }

  @Override
  public T map(String toBeParsed) throws Exception {
    reusableToConvert.updateByParse(toBeParsed);
    return reusableToConvert;
  }
}
