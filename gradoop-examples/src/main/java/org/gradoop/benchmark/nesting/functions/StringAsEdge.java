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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;

/**
 * Created by vasistas on 08/04/17.
 */
public class StringAsEdge implements FlatMapFunction<String, ImportEdge<String>> {

  private final ImportEdge<String> reusable;

  public StringAsEdge() {
    reusable = new ImportEdge<>();
    reusable.setProperties(new Properties());
  }

  @Override
  public void flatMap(String value, Collector<ImportEdge<String>> out) throws Exception {
    String[] arguments = value.split(" ");
    if (arguments.length == 3) {
      reusable.setId(value);
      reusable.setSourceId(arguments[0]);
      reusable.setLabel(arguments[1]);
      reusable.setTargetId(arguments[2]);
      out.collect(reusable);
    }
  }
}
