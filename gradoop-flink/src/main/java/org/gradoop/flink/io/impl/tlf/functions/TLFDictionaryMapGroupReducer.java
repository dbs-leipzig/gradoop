
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
