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

package org.gradoop.io.impl.tsv.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.properties.PropertyValue;

import java.util.ArrayList;
import java.util.List;

/**
 * GroupReduceFunction to Reduce duplicated original id's from the vertex set
 *
 * @param <V> EPGM vertex type class
 */
public class VertexReducer<V extends EPGMVertex> implements
  GroupReduceFunction<V, V> {
  /**
   * Reduces duplicated original id's from vertex set
   *
   * @param iterable    vertex set
   * @param collector   reduced collection
   * @throws Exception
   */
  @Override
  public void reduce(Iterable<V> iterable, Collector<V> collector) throws
    Exception {
    List<PropertyValue> ids = new ArrayList<>();
    for (V vex : iterable) {
      if (!ids.contains(vex.getPropertyValue("id"))) {
        collector.collect(vex);
      }
      ids.add(vex.getPropertyValue("id"));
    }
  }
}
