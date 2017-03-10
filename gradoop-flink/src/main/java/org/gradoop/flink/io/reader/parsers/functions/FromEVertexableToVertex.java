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
 *
 *
 * @param <Id>  Defining the elements' id
 * @param <E>   Defining the temporary edge
 *
 */
public class FromEVertexableToVertex<Id extends Comparable<Id>,
                                     E extends Vertexable<Id>>

                          implements MapFunction<E, ImportVertex<Id>> {

  /**
   * Reusable element
   */
  private final ImportVertex<Id> reusableToReturn = new ImportVertex<Id>();

  @Override
  public ImportVertex<Id> map(E toBeParsed) throws Exception {
    reusableToReturn.setId(toBeParsed.getId());
    reusableToReturn.setProperties(toBeParsed);
    reusableToReturn.setLabel(toBeParsed.getLabel());
    return reusableToReturn;
  }
}
