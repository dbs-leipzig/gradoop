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

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.api.entities.EPGMGraphElement;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.GraphHeadFactory;
import org.gradoop.flink.io.impl.csv.CSVConstants;

/**
 * Creates a graph head for each graph key of an epgm graph element.
 *
 * @param <T> an epgm graph element: vertex or edge
 */
public class ElementGraphKeyToGraphHead<T extends EPGMGraphElement>
  implements GroupReduceFunction<Tuple2<T, String>, GraphHead> {
  /**
   * EPGM graph head factory
   */
  private GraphHeadFactory graphHeadFactory;

  /**
   * Creates a group reduce function.
   *
   * @param graphHeadFactory epgm graph head factory
   */
  public ElementGraphKeyToGraphHead(GraphHeadFactory graphHeadFactory) {
    this.graphHeadFactory = graphHeadFactory;
  }

  @Override
  public void reduce(Iterable<Tuple2<T, String>> iterable, Collector<GraphHead> collector)
    throws Exception {
    GraphHead graphHead = graphHeadFactory.createGraphHead();
    graphHead.setProperty(CSVConstants.PROPERTY_KEY_KEY, iterable.iterator().next().f1);
    collector.collect(graphHead);
  }
}
