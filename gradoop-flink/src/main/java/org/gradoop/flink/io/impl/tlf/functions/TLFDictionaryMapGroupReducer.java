/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.io.impl.tlf.functions;


import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;

import java.util.Map;

/**
 * Reduces the incoming list of vertex or edge labels to a single HashMap
 * containing those labels without duplicates and an ascending integer value.
 */
public class TLFDictionaryMapGroupReducer
  implements GroupReduceFunction<String, Map<String, Integer>> {

  @Override
  public void reduce(Iterable<String> iterable, Collector<Map<String, Integer>> collector)
    throws Exception {
    Map<String, Integer> newMap = Maps.newHashMap();
    int id = 0;
    for (String label : iterable) {
      newMap.put(label, id);
      id++;
    }
    collector.collect(newMap);
  }
}
