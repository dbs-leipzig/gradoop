/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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
package org.gradoop.dataintegration.importer.impl.csv;

import org.apache.flink.api.java.DataSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.dataintegration.importer.impl.csv.functions.CsvRowToProperties;
import org.gradoop.dataintegration.importer.impl.csv.functions.PropertiesToVertex;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Read a csv file and import each row as a vertex in EPGM representation.
 */
public class MinimalCSVImporter implements DataSource {

  /**
   * Token delimiter of the CSV file.
   */
  private String tokenSeparator;

  /**
   * Path to the csv file.
   */
  private String path;

  /**
   * The charset used in the csv file, e.g., "UTF-8".
   */
  private String charset;

  /**
   * The property names for all columns in the file. If {@code null}, the first line will be
   * interpreted as header row.
   */
  private List<String> columnNames;

  /**
   * Flag to specify if each row of the file should be checked for reoccurring of the column
   * property names.
   */
  private boolean checkReoccurringHeader;

  /**
   * Gradoop Flink configuration.
   */
  private GradoopFlinkConfig config;

  /**
   * Create a new MinimalCSVImporter instance by the given parameters.
   *
   * @param path the path to the csv file
   * @param tokenSeparator the token delimiter of the csv file
   * @param config GradoopFlinkConfig configuration
   * @param columnNames property identifiers for each column
   * @param checkReoccurringHeader if each row of the file should be checked for reoccurring of
   *                               the column property names
   */
  public MinimalCSVImporter(String path, String tokenSeparator, GradoopFlinkConfig config,
                            List<String> columnNames, boolean checkReoccurringHeader) {
    this(path, tokenSeparator, config, checkReoccurringHeader);
    this.columnNames = Objects.requireNonNull(columnNames);
  }

  /**
   * Create a new MinimalCSVImporter instance by the given parameters.
   * Use the UTF-8 charset as default. The first line of the file will be set as the property
   * names for each column.
   *
   * @param path the path to the csv file
   * @param tokenSeparator the token delimiter of the csv file
   * @param config GradoopFlinkConfig configuration
   * @param checkReoccurringHeader if each row of the file should be checked for reoccurring of
   *                               the column property names.
   */
  public MinimalCSVImporter(String path, String tokenSeparator, GradoopFlinkConfig config,
                            boolean checkReoccurringHeader) {
    this(path, tokenSeparator, config, "UTF-8", checkReoccurringHeader);
  }

  /**
   * Create a new MinimalCSVImporter with a user set charset. The first line of the file
   * will set as the property names for each column.
   *
   * @param path the path to the csv file
   * @param tokenSeparator the token delimiter of the csv file
   * @param config GradoopFlinkConfig configuration
   * @param charset the charset used in the csv file
   * @param checkReoccurringHeader if each row of the file should be checked for reoccurring of
   *                               the column property names.
   */
  public MinimalCSVImporter(String path, String tokenSeparator, GradoopFlinkConfig config,
                            String charset, boolean checkReoccurringHeader) {
    this.path = Objects.requireNonNull(path);
    this.tokenSeparator = Objects.requireNonNull(tokenSeparator);
    this.config = Objects.requireNonNull(config);
    this.charset = Objects.requireNonNull(charset);
    this.checkReoccurringHeader = checkReoccurringHeader;
  }

  /**
   * Import each row of the file as a vertex and create a logical graph from it.
   * If no column property names are set, read the first line of the file as header and set this
   * values as column names.
   *
   * @return a logical graph with a vertex per csv line and no edges
   * @throws IOException on failure
   */
  @Override
  public LogicalGraph getLogicalGraph() throws IOException {
    DataSet<Vertex> vertices;

    if (columnNames == null) {
      vertices = readCSVFile(readHeaderRow(), checkReoccurringHeader);
    } else {
      vertices = readCSVFile(columnNames, checkReoccurringHeader);
    }

    return config.getLogicalGraphFactory().fromDataSets(vertices);
  }

  @Override
  public GraphCollection getGraphCollection() throws IOException {
    return config.getGraphCollectionFactory().fromGraph(getLogicalGraph());
  }

  /**
   * Reads the csv file specified by {@link MinimalCSVImporter#path} and converts each valid line
   * to a {@link Vertex}.
   *
   * @param propertyNames list of the property identifier names
   * @param checkReoccurringHeader set to true if each row of the file should be checked for
   *                               reoccurring of the column property names
   * @return a {@link DataSet} of all vertices from one specific file
   */
  private DataSet<Vertex> readCSVFile(List<String> propertyNames, boolean checkReoccurringHeader) {
    return config.getExecutionEnvironment()
      .readTextFile(path)
      .flatMap(new CsvRowToProperties(tokenSeparator, propertyNames, checkReoccurringHeader))
      .map(new PropertiesToVertex<>(config.getVertexFactory()))
      .returns(config.getVertexFactory().getType());
  }

  /**
   * Reads the fist row of a csv file and creates a list including all column entries that
   * will be used as property names.
   *
   * @return the property names
   * @throws IOException if an error occurred while open the stream
   */
  private List<String> readHeaderRow() throws IOException {
    Path filePath = new Path(path);
    try (FileSystem fs = FileSystem.get(filePath.toUri(), new Configuration())) {
      FSDataInputStream inputStream = fs.open(filePath);
      BufferedReader lineReader = new BufferedReader(new InputStreamReader(inputStream, charset));
      String headerLine = lineReader.readLine();
      lineReader.close();
      if (headerLine == null || headerLine.isEmpty()) {
        throw new IOException("The csv file '" + path + "' does not contain any rows.");
      }
      return Arrays.asList(headerLine.split(tokenSeparator));
    } catch (IOException ioe) {
      throw new IOException("Error while opening a stream to '" + path + "'.", ioe);
    }
  }
}
