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
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.PropertyValue;

/**
 * Map function to split a line of a csv file in a tuple of an external vertex.
 *
 * Output fields:
 * f0: vertex id
 * f1: vertex label
 * f2: vertex properties
 */
public class MapCSVLineToVertex implements MapFunction<String, Tuple3<String, String, Properties>> {

  /**
   * Token separator for the csv file.
   */
  private String tokenSeparator;
  /**
   * Map the path of an vertex file to the vertex property names.
   */
  private Map<String, List<String>> propertyMap;
  /**
   * The path to the vertex file.
   */
  private String vertexPath;

  /**
   * Create a new map function.
   *
   * @param tokenSeparator token separator
   * @param propertyMap map edge path to edge properties
   * @param vertexPath file path
   */
  public MapCSVLineToVertex(String tokenSeparator,
          Map<String, List<String>> propertyMap, String vertexPath) {
    this.tokenSeparator = tokenSeparator;
    this.propertyMap = propertyMap;
    this.vertexPath = vertexPath;
  }

  @Override
  public Tuple3<String, String, Properties> map(String line) throws Exception {
    String[] tokens = line.split(tokenSeparator, 3);
    Properties props = parseProperties(tokens[2], propertyMap.get(vertexPath));
    return Tuple3.of(tokens[0], tokens[1], props);
  }

  /**
   * Map each label to the occurring properties.
   *
   * @param propertyValueString the properties
   * @param propertyLabels List of all property names.
   * @return Properties as pojo element
   */
  public Properties parseProperties(String propertyValueString,
          List<String> propertyLabels) {

    Properties properties = new Properties();

    String[] propertyValues = propertyValueString.split(tokenSeparator);

    for (int i = 0; i < propertyValues.length; i++) {
      if (propertyValues[i].length() > 0) {
        properties.set(propertyLabels.get(i),
            PropertyValue.create(propertyValues[i]));
      }
    }

    return properties;
  }
}
