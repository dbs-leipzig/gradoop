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

package org.gradoop.flink.model.impl.operators.distinction.functions;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.flink.model.api.functions.DistinctionFunction;

import java.util.Iterator;

/**
 * Distinction function that just selects the first graph head of an isomorphic group.
 */
public class CountGraphHeads implements DistinctionFunction {

  /**
   * property key to store graph count.
   */
  private final String propertyKey;

  /**
   * Constructor.
   *
   * @param propertyKey property key to store graph count.
   */
  public CountGraphHeads(String propertyKey) {
    this.propertyKey = propertyKey;
  }

  @Override
  public void reduce(Iterable<Tuple2<String, GraphHead>> iterable,
    Collector<GraphHead> collector) throws Exception {
    Iterator<Tuple2<String, GraphHead>> iterator = iterable.iterator();

    GraphHead graphHead = iterator.next().f1;
    int count = 1;

    while (iterator.hasNext()) {
      iterator.next();
      count++;
    }

    graphHead.setProperty(propertyKey, count);

    collector.collect(graphHead);
  }
}
