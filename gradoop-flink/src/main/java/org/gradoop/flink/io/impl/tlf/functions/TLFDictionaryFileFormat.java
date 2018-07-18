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
