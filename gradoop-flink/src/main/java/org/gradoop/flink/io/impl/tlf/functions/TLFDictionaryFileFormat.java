
package org.gradoop.flink.io.impl.tlf.functions;

import org.apache.flink.api.java.io.TextOutputFormat;

import java.util.Map;

/**
 * Creates a dictionary format from a GraphTransaction as follows:
 * <p>
 *   label0 0
 *   label1 1
 *   ...
 * </p>
 */
public class TLFDictionaryFileFormat implements
  TextOutputFormat.TextFormatter<Map<String, Integer>> {

  /**
   * Creates a TLF dictionary string representation of a given graph
   * transaction.
   *
   * @param map map containing the labels and their ids
   * @return TLF dictionary string representation
   */
  @Override
  public String format(Map<String, Integer> map) {
    StringBuilder dictionary = new StringBuilder();
    for (Map.Entry<String, Integer> entry : map.entrySet()) {
      dictionary.append(entry.getKey() + " " + entry.getValue() + "\n");
    }
    return dictionary.toString();
  }
}
