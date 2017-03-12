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
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;
import org.gradoop.flink.io.reader.parsers.inputfilerepresentations.Vertexable;

/**
 * Converts an object that could be represented as a vertex (Vertexable) into a
 * proper vertex instance.
 * @param <K> comparable element
 */
public class ToVertex<K extends Comparable<K>> implements MapFunction<String,
  ImportVertex<K>> {

  /**
   * Reusable element
   */
  private final ImportVertex<K> reusableToReturn = new ImportVertex<K>();

  /**
   * Intermediate representation and parser
   */
  private final Vertexable<K> reusableToConvert;

  /**
   * Default constructor
   * @param reusableToConvert Intermediate representation and parser
   */
  public ToVertex(Vertexable<K> reusableToConvert) {
    this.reusableToConvert = reusableToConvert;
  }

  @Override
  public ImportVertex<K> map(String toBeParsed) throws Exception {
    reusableToConvert.updateByParse(toBeParsed);
    reusableToReturn.setId(reusableToConvert.getId());
    reusableToReturn.setProperties(reusableToConvert);
    reusableToReturn.setLabel(reusableToConvert.getLabel());
    return reusableToReturn;
  }
}
