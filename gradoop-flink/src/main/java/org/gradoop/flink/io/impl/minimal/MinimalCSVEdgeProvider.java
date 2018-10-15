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
package org.gradoop.flink.io.impl.minimal;

import java.util.List;
import java.util.Map;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple5;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.io.api.EdgeImporter;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;
import org.gradoop.flink.io.impl.minimal.functions.CreateLabeledImportEdgeProperties;
import org.gradoop.flink.io.impl.minimal.functions.MapCSVLineToEdge;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * Import external edges from csv files into EPGM.
 * It is possible import edges from different csv files.
 */
public class MinimalCSVEdgeProvider implements EdgeImporter<String> {

  /**
   * Token separator of the file.
   */
  private String tokenSeparator;
  /**
   * Map the file to the containing property names.
   */
  private Map<String, List<String>> propertyMap;
  /**
   * Gradoop Flink configuration
   */
  private GradoopFlinkConfig config;
  /**
   * DataSet of edges in EPGM format
   */
  private DataSet<ImportEdge<String>> importEdge;

  /**
   * Constructor
   *
   * @param propertyMap Map of the file path and the property names.
   * @param tokenSeparator token separator
   * @param config Gradoop Flink configuration
   */
  public MinimalCSVEdgeProvider(Map<String, List<String>> propertyMap,
      String tokenSeparator, GradoopFlinkConfig config) {
    this.config = config;
    this.tokenSeparator = tokenSeparator;
    this.propertyMap = propertyMap;
  }

  /**
   * Import the external edges into EPGM.
   * Combine each edges from different files into one DataSet.
   *
   * @return DataSet of all edges of the graph.
   */
  public DataSet<ImportEdge<String>> importEdge() {
    importEdge = null;
    for (Map.Entry<String, List<String>> entry : propertyMap.entrySet()) {
      if (importEdge != null) {
        importEdge = importEdge.union(readCSVFile(config, entry.getKey(), tokenSeparator));
      } else {
        importEdge = readCSVFile(config, entry.getKey(), tokenSeparator);
      }
    }
    return importEdge;
  }

  /**
   * Read the edges from a csv file.
   *
   * @param config Gradoop Flink configuration
   * @param edgeCsvPath path to the file
   * @param tokenSeparator separator
   * @return DateSet of all edges from one specific file.
   */
  public DataSet<ImportEdge<String>> readCSVFile(
      GradoopFlinkConfig config, String edgeCsvPath, String tokenSeparator) {
    DataSet<Tuple5<String, String, String, String, Properties>> lines =
            config.getExecutionEnvironment()
            .readTextFile(edgeCsvPath)
            .map(new MapCSVLineToEdge(tokenSeparator, propertyMap, edgeCsvPath));

    return lines.map(new CreateLabeledImportEdgeProperties<>());
  }

  @Override
  public DataSet<ImportEdge<String>> getImportEdge() {
    return importEdge;
  }
}

