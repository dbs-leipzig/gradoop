
package org.gradoop.flink.io.impl.tlf.functions;

import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.Map;

/**
 * Converts a dataset of tuples of integer and string into a dataset which
 * contains only one map from integer to string.
 */
public class Dictionary
  implements GroupReduceFunction<Tuple2<Integer, String>, Map<Integer, String>>
{

  /**
   * Reduces the Tuple2 iterable into one map.
   *
   * @param iterable containing tuples of integer and string
   * @param collector collects one map from integer to string
   * @throws Exception
   */
  @Override
  public void reduce(
    Iterable<Tuple2<Integer, String>> iterable,
    Collector<Map<Integer, String>> collector) throws Exception {
    Map<Integer, String> dictionary = Maps.newHashMap();
    for (Tuple2<Integer, String> tuple : iterable) {
      dictionary.put(
        (Integer) tuple.getField(0), (String) tuple.getField(1));
    }
    collector.collect(dictionary);
  }
}
