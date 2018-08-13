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
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.io.api.VertexImporter;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;
import org.gradoop.flink.io.impl.minimal.functions.CreateLabeledImportVertexProperties;
import org.gradoop.flink.io.impl.minimal.functions.MapCSVLineToVertex;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * Import external vertices from csv files into EPGM.
 * It is possible import vertices from different csv files.
 */
public class MinimalCSVVertexProvider implements VertexImporter<String> {

  /**
   * Token delimiter
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
   * Set of vertices in EPGM format.
   */
  private DataSet<ImportVertex<String>> importVertices;

  /**
   * Constructor.
   *
   * @param config Gradoop Flink configuration
   * @param tokenSeparator Delimiter of csv file
   * @param propertyMap Map of file path and property names
   */
  public MinimalCSVVertexProvider(Map<String, List<String>> propertyMap,
        String tokenSeparator, GradoopFlinkConfig config) {
    this.propertyMap = propertyMap;
    this.tokenSeparator = tokenSeparator;
    this.config = config;
  }

  /**
   * Import the external vertices into EPGM.
   * Combine each vertices from different files into one DataSet.
   *
   * @return DataSet of all vertices of the graph.
   */
  public DataSet<ImportVertex<String>> importVertex() {
    importVertices = null;
    for (Map.Entry<String, List<String>> entry : propertyMap.entrySet()) {
      if (importVertices != null) {
        importVertices = importVertices.union(readCSVFile(config, entry.getKey(), tokenSeparator));
      } else {
        importVertices = readCSVFile(config, entry.getKey(), tokenSeparator);
      }
    }
    return importVertices;
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
                .map(new MapCSVLineToVertex(tokenSeparator, propertyMap, vertexCsvPath));

    return lines.map(new CreateLabeledImportVertexProperties<>());
  }

  @Override
  public DataSet<ImportVertex<String>> getImportVertex() {
    return importVertices;
  }
}
