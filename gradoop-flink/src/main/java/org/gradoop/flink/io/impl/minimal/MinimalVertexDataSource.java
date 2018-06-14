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

import java.io.IOException;
import java.util.List;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.edgelist.functions.CreateLabeledImportVertex;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * Data source to create an logical graph from an external CSV format into Gradoop.
 */
public class MinimalVertexDataSource implements DataSource {

  /**
   * Path to the vertex csv file
   */
  private String vertexCsvPath;

  /**
   * Gradoop Flink configuration
   */
  private GradoopFlinkConfig config;

  /**
   * Token delimiter
   */
  private String tokenSeparator;

  /**
   * Name of the id column
   */
  private String vertexIdColumn;

  /**
   * Name of the label column
   */
  private String vertexLabel;

  /**
   * Property names of all vertices
   */
  private List<String> vertexProperties;

  /**
   * Creates a new data source.
   *
   * @param vertexPath Path to the vertex file
   * @param config Gradoop Flink configuration
   * @param tokenSeparator Delimiter of csv file
   * @param vertexIdColumn Name of the id column
   * @param labelColumn Name of the label column
   * @param vertexProperties List of all property names
   */
  public MinimalVertexDataSource(String vertexPath, GradoopFlinkConfig config,
      String tokenSeparator, String vertexIdColumn, String labelColumn,
      List<String> vertexProperties) {
    this.vertexCsvPath = vertexPath;
    this.config = config;
    this.tokenSeparator = tokenSeparator;
    this.vertexIdColumn = vertexIdColumn;
    this.vertexLabel = labelColumn;
    this.vertexProperties = vertexProperties;
  }

  @Override
  public LogicalGraph getLogicalGraph() throws IOException {

    ExecutionEnvironment env = getConfig().getExecutionEnvironment();

 // DataSet<ImportVertex<String>> vertices = env.fromElements(map(
 //     env.readTextFile(vertexCsvPath)));

    DataSet<Tuple3<String, String, String>> lineTuples = env
     .readTextFile(vertexCsvPath)
     .map(line -> {
         String[] tokens = line.split(tokenSeparator, 3);
         return Tuple3.of(tokens[0], tokens[1], tokens[2]);
       })
     .returns(new TypeHint<Tuple3<String, String, String>>() { });

    DataSet<ImportVertex<String>> importVertices = lineTuples
        .<Tuple2<String, String>>project(0, 1)
        .map(new CreateLabeledImportVertex<>(vertexLabel));

    return null;
  }

  /**
   * Map a csv line to a EPMG vertex
   * @param csvLine external representation of the vertex
   * @return EPMG vertex
   */
  public ImportVertex<String> map(String csvLine) {

    String[] tokens = csvLine.split(tokenSeparator, 3);
    return new ImportVertex<String>(tokens[0],
        tokens[1],
        parseProperties(tokens[1], tokens[2]));
  }

  /**
   * Map each label to the occurring properties.
   * @param label Name of the label
   * @param propertyValueString the properties
   * @return Properties as pojo element
   */
  public Properties parseProperties(String label, String propertyValueString) {

    Properties properties = new Properties();

    String[] propertyValues = propertyValueString.split(tokenSeparator);

    for (int i = 0; i < propertyValues.length; i++) {
      if (propertyValues[i].length() > 0) {
        properties.set(vertexProperties.get(i),
            PropertyValue.create(propertyValues[i]));
      }
    }
    return properties;
  }

  @Override
  public GraphCollection getGraphCollection() throws IOException {

    return null;
  }

  /**
   * Getter of config
   * @return config
   */
  GradoopFlinkConfig getConfig() {
    return config;
  }

  /**
   * Getter of vertexCsvPath
   * @return vertexCsvPath
   */
  String getVertexCsvPath() {
    return vertexCsvPath;
  }

  /**
   * Getter of tokenSeperator
   * @return tokenSeperator
   */
  String getTokenSeparator() {
    return tokenSeparator;
  }

  /**
   * Getter of vertexProperties
   * @return vertexProperties
   */
  List<String> getVertexProperties() {
    return vertexProperties;
  }

}
