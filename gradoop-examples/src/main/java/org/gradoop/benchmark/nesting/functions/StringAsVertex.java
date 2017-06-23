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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;

/**
 * Maps a string into a vertex
 */
@FunctionAnnotation.ForwardedFields("* -> f0")
public class StringAsVertex implements MapFunction<String, ImportVertex<String>> {

  /**
   * Reusable element
   */
  private final ImportVertex<String> reusable;

  /**
   * Default constructor
   */
  public StringAsVertex() {
    reusable = new ImportVertex<>();
    reusable.setProperties(new Properties());
    reusable.setLabel("");
  }

  @Override
  public ImportVertex<String> map(String value) throws Exception {
    reusable.setId(value);
    reusable.setLabel(value);
    return reusable;
  }
}
