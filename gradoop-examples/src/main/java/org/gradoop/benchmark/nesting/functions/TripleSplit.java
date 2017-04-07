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

package org.gradoop.benchmark.nesting.functions;

import org.gradoop.benchmark.nesting.parsers.ConvertStringToEdge;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;

/**
 * Parsing an edge, skipping empty strings
 */
public class TripleSplit implements ConvertStringToEdge<String, String> {

  /**
   * Reusable element
   */
  private final ImportEdge<String> reusable;

  /**
   * Default constructor
   */
  public TripleSplit() {
    this.reusable = new ImportEdge<>();
  }

  @Override
  public boolean isValid(String toCheck) {
    return toCheck.length() > 0;
  }

  @Override
  public ImportEdge<String> map(String s) throws Exception {
    String[] array = s.split(" ");
    reusable.setLabel(array[1]);
    reusable.setSourceId(array[0]);
    reusable.setTargetId(array[2]);
    reusable.setId(s);
    return reusable;
  }
}
