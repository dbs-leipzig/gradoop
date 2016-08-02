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

package org.gradoop.flink.io.impl.tlf.functions;


import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

/**
 * Reduces the incoming list of vertex or edge labels to a single HashMap
 * containing those labels without duplicates and an ascending integer value.
 */
public class TLFDictionaryMapGroupReducer implements
  GroupReduceFunction<List<String>, Map<String,
  Integer>> {

  @Override
  public void reduce(Iterable<List<String>> iterable,
    Collector<Map<String, Integer>> collector) throws Exception {
    Map<String, Integer> newMap = Maps.newHashMap();
    int id = 0;
    for (List<String> list : iterable) {
      for (int i = 0; i < list.size(); i++) {
        if (!newMap.containsKey(list.get(i))) {
          newMap.put(list.get(i), id);
          id++;
        }
      }
    }
    collector.collect(newMap);
  }
}
