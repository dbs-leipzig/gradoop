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

package org.gradoop.flink.io.impl.csv.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.api.entities.EPGMGraphElement;
import org.gradoop.flink.io.impl.csv.CSVConstants;

/**
 * Extracts all graph keys from an epgm graph element.
 *
 * @param <T> an epgm graph element: vertex or edge
 */
public class ElementToElementGraphKey<T extends EPGMGraphElement>
  implements FlatMapFunction<T, String> {

  @Override
  public void flatMap(T element, Collector<String> collector)
      throws Exception {
    String graphs = element.getPropertyValue(CSVConstants.PROPERTY_KEY_GRAPHS).getString();
    for (String graph : graphs.split(CSVConstants.SEPARATOR_GRAPHS)) {
      graph = graph.replaceAll(CSVConstants.ESCAPE_SEPARATOR_GRAPHS,
        CSVConstants.SEPARATOR_GRAPHS);
      collector.collect(graph);
    }
  }
}
