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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.GraphHead;

/**
 * Maps the graph head to a tuple of graph key and graphhead. The key is taken from the heads key
 * property.
 */
public class GraphHeadToGraphKeyGraphHead
  implements MapFunction<GraphHead, Tuple2<String, GraphHead>> {

  @Override
  public Tuple2<String, GraphHead> map(GraphHead graphHead) throws Exception {
    return new Tuple2<String, GraphHead>(graphHead.getPropertyValue("key").getString(), graphHead);
  }
}
