/**
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
package org.gradoop.flink.io.impl.minimal;

import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;
import org.gradoop.flink.io.impl.minimal.functions.CreateLabeledImportVertexProperties;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * Import external vertices from csv files into EPGM.
 * It is possible import vertices from different csv files.
 */
public class MinimalVertexProvider {

  /**
   * Token delimiter
   */
  private static String TOKEN_SEPERATOR;

  /**
   * Map the file to the containing property names.
   */
  private static Map<String, List<String>> PROPERTY_MAP;

  /**
   * Gradoop Flink configuration
   */
  private GradoopFlinkConfig config;

  /**
   * Constructor.
   * @param config Gradoop Flink configuration
   * @param tokenSeperator Delimiter of csv file
   * @param propertyMap Map of file path and property names
   */
  public MinimalVertexProvider(Map<String, List<String>> propertyMap,
        String tokenSeperator, GradoopFlinkConfig config) {
    this.PROPERTY_MAP = propertyMap;
    this.TOKEN_SEPERATOR = tokenSeperator;
    this.config = config;
  }

  /**
   * Import the external vertices into EPGM.
   * Combine each vertices from different files into one DataSet.
   * @return DataSet of all vertices of the graph.
   */
  public DataSet<ImportVertex<String>> importVertex() {

    DataSet<ImportVertex<String>> vertices = null;
    for (Map.Entry<String, List<String>> entry : PROPERTY_MAP.entrySet()) {
      if (vertices != null) {
        vertices = vertices.union(readCSVFile(config, entry.getKey(), TOKEN_SEPERATOR));
      } else {
        vertices = readCSVFile(config, entry.getKey(), TOKEN_SEPERATOR);
      }
    }
    return vertices;
  }

  /**
   * Read the vertices from a csv file.
   *
   * @param config Gradoop Flink configuration
   * @param vertexCsvPath path to the file
   * @param tokenSeparator separator
   * @return DateSet of all vertices from one specific file.
   */
  public DataSet<ImportVertex<String>> readCSVFile(
      GradoopFlinkConfig config, String vertexCsvPath, String tokenSeparator) {

    DataSet<Tuple3<String, String, Properties>> lines = config.getExecutionEnvironment()
                .readTextFile(vertexCsvPath)
                .map(line -> {
                    String[] tokens = line.split(tokenSeparator, 3);
                    Properties props = parseProperties(tokens[2],
                            PROPERTY_MAP.get(vertexCsvPath));
                    return Tuple3.of(tokens[0], tokens[1], props);
                  }).returns(new TypeHint<Tuple3<String, String, Properties>>() { });

    DataSet<ImportVertex<String>> vertices = lines.map(new CreateLabeledImportVertexProperties<>());

    return vertices;
  }

  /**
   * Map each label to the occurring properties.
   * @param propertyValueString the properties
   * @param propertieLabels List of all property names.
   * @return Properties as pojo element
   */
  public static Properties parseProperties(String propertyValueString,
          List<String> propertieLabels) {

    Properties properties = new Properties();

    String[] propertyValues = propertyValueString.split(TOKEN_SEPERATOR);

    for (int i = 0; i < propertyValues.length; i++) {
      if (propertyValues[i].length() > 0) {
        properties.set(propertieLabels.get(i),
            PropertyValue.create(propertyValues[i]));
      }
    }

    return properties;
  }
}
