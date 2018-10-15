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
package org.gradoop.flink.io.impl.minimal.functions;

import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.PropertyValue;

/**
 * Map function to split a line of a csv file in a tuple of an external edge.
 *
 * Output fields:
 * f0: edge id
 * f1.f0: source id
 * f1.f1: target id
 * f2: edge label
 * f3: edge properties
 */
public class MapCSVLineToEdge
  implements MapFunction<String, Tuple5<String, String, String, String, Properties>> {

  /**
   * Token separator for the csv file.
   */
  private String tokenSeparator;

  /**
   * Map the path of an edge file to the edge property names.
   */
  private Map<String, List<String>> propertyMap;

  /**
   * The path to the edge file.
   */
  private String edgePath;

  /**
   * Create a new map function.
   *
   * @param tokenSeparator token separator
   * @param propertyMap map file path to the properties
   * @param edgePath path of the file
   */
  public MapCSVLineToEdge(String tokenSeparator,
          Map<String, List<String>> propertyMap, String edgePath) {
    this.tokenSeparator = tokenSeparator;
    this.propertyMap = propertyMap;
    this.edgePath = edgePath;
  }

  @Override
  public Tuple5<String, String, String, String, Properties> map(String line)
    throws Exception {
    String[] tokens = line.split(tokenSeparator, 5);
    Properties props = parseProperties(tokens[4], propertyMap.get(edgePath));
    return Tuple5.of(tokens[0], tokens[1], tokens[2], tokens[3], props);
  }

  /**
   * Map each label to the occurring properties.
   *
   * @param propertyValueString the properties
   * @param propertyLabels List of all property names.
   * @return Properties as pojo element
   */
  public Properties parseProperties(String propertyValueString, List<String> propertyLabels) {

    Properties properties = new Properties();

    String[] propertyValues = propertyValueString.split(tokenSeparator);

    for (int i = 0; i < propertyValues.length; i++) {
      if (propertyValues[i].length() > 0) {
        properties.set(propertyLabels.get(i), PropertyValue.create(propertyValues[i]));
      }
    }

    return properties;
  }
}
