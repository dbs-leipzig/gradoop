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

package org.gradoop.flink.io.impl.xmlbasedcsv.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.GraphHeadFactory;
import org.gradoop.flink.io.impl.xmlbasedcsv.XMLBasedCSVConstants;

/**
 * Selects only the tuple where the graphhead is not null or creates a new graphhead if now graph
 * key has an assigned graphhead. The input is grouped by Tuple.f0.
 */
public class DistinctGraphKeysWithHead
  implements GroupReduceFunction<Tuple2<String, GraphHead>, Tuple2<String, GraphHead>> {
  /**
   * EPGM graph head factory
   */
  private GraphHeadFactory graphHeadFactory;

  /**
   * Valued constructor.
   *
   * @param graphHeadFactory EPGM graph head factory
   */
  public DistinctGraphKeysWithHead(GraphHeadFactory graphHeadFactory) {
    this.graphHeadFactory = graphHeadFactory;
  }

  @Override
  public void reduce(Iterable<Tuple2<String, GraphHead>> iterable,
    Collector<Tuple2<String, GraphHead>> collector) throws Exception {
    Tuple2<String, GraphHead> result = null;
    // if possible select the key which already has a mapped graphhead
    for (Tuple2<String, GraphHead> tuple : iterable) {
      if (result == null) {
        result = tuple;
      }
      if (tuple.f1 != null) {
        result = tuple;
      }
    }
    // if the graph key does not have a mapped graphhead, one is created
    if (result.f1 == null) {
      GraphHead graphHead = graphHeadFactory.createGraphHead();
      graphHead.setProperty(XMLBasedCSVConstants.PROPERTY_KEY_KEY, result.f0);
      result.f1 = graphHead;
    }
    collector.collect(result);
  }
}
