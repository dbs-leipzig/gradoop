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

import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.api.entities.EPGMGraphElement;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.io.impl.csv.CSVConstants;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Sets the graph ids for each element depending on the graph keys.
 *
 * @param <T> an epgm graph element: vertex or edge
 */
public class SetElementGraphIds<T extends EPGMGraphElement>
  extends RichGroupReduceFunction<Tuple2<T, String>, T> {
  /**
   * Map from string representation of the graph key to its id.
   */
  private Map<String, GradoopId> keyIdMap;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    keyIdMap = getRuntimeContext().<HashMap<String, GradoopId>>getBroadcastVariable(
      CSVConstants.BROADCAST_GRAPHHEADS).get(0);
  }

  @Override
  public void reduce(Iterable<Tuple2<T, String>> iterable, Collector<T> collector)
    throws Exception {
    Iterator<Tuple2<T, String>> iterator = iterable.iterator();
    //every tuple contains the same element
    Tuple2<T, String> tuple = iterator.next();
    T element = tuple.f0;
    //add the first graph id
    element.addGraphId(keyIdMap.get(tuple.f1));
    //add all other graph ids, if existing
    while (iterator.hasNext()) {
      tuple = iterator.next();
      element.addGraphId(keyIdMap.get(tuple.f1));
    }
    collector.collect(element);
  }

}
