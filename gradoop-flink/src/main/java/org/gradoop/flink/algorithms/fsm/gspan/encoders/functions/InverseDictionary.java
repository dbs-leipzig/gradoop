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

package org.gradoop.flink.algorithms.fsm.gspan.encoders.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

/**
 * [s,..] => {0=s,..}
 */
public class InverseDictionary
  implements MapFunction<List<String>, Map<String, Integer>> {

  @Override
  public Map<String, Integer> map(List<String> intStringDictionary) throws
    Exception {

    int labelCount = intStringDictionary.size();

    Map<String, Integer> stringIntDictionary =
      Maps.newHashMapWithExpectedSize(labelCount);

    for (int i = 0; i < labelCount; i++) {
      stringIntDictionary.put(intStringDictionary.get(i), i);
    }

    return stringIntDictionary;
  }
}
