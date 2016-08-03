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
