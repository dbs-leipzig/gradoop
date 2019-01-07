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
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.dataintegration.importer.impl.csv.functions.CreateImportVertexCSV;
import org.gradoop.dataintegration.importer.impl.csv.functions.RowToVertexMapper;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Read a csv file and import each row as a vertex in EPGM representation.
 */
public class MinimalCSVImporter {

  /**
   * Token delimiter
   */
  private String tokenSeparator;

  /**
   * Path to the csv file
   */
  private String path;

  /**
   * The charset used in the csv file.
   */
  private String charset;

  /**
   * THe property names for all columns in the file.
   */
  private List<String> columnNames;

  /**
   * set to true if each row of the file should be checked for
   * reoccurring of the column property names
   */
  private boolean checkReoccurringHeader;

  /**
   * Gradoop Flink configuration
   */
  private GradoopFlinkConfig config;

  /**
   * Create a new MinimalCSVImporter. The user set a list of the property names.
   *
   * @param path the path to the csv file
   * @param tokenSeparator the token delimiter of the csv file
   * @param config GradoopFlinkConfig configuration
   * @param columnNames property identifier for each column
   * @param checkReoccurringHeader if each row of the file should be checked for reoccurring of
   *                               the column property names.
   */
  public MinimalCSVImporter(String path, String tokenSeparator, GradoopFlinkConfig config,
                            List<String> columnNames, boolean checkReoccurringHeader) {
    this(path, tokenSeparator, config, checkReoccurringHeader);
    this.columnNames = Objects.requireNonNull(columnNames);
  }

  /**
   * Create a new MinimalCSVImporter. Use the utf-8 charset as default. The first line of the file
   * will set as the property names for each column.
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
   * Import each row of the file as a vertex. If no column property names are set,
   * read the first line of the file as header and set this values as column names.
   *
   * @return the imported vertices
   * @throws IOException if an error occurred while open the stream
   */
  public DataSet<Vertex> importVertices()
    throws IOException {
    if (columnNames == null) {
      return readCSVFile(readHeaderRow(), checkReoccurringHeader);
    } else {
      return readCSVFile(columnNames, checkReoccurringHeader);
    }
  }

  /**
   * Reads the csv file and converts each valid line to a import vertex.
   *
   * @param propertyNames list of the property identifier names
   * @param checkReoccurringHeader set to true if each row of the file should be checked for
   *                               reoccurring of the column property names
   * @return a DataSet of all import vertices from one specific file
   */
  private DataSet<Vertex> readCSVFile(List<String> propertyNames,
                                                  boolean checkReoccurringHeader) {

    DataSet<Properties> lines = config.getExecutionEnvironment()
      .readTextFile(path)
      .flatMap(new RowToVertexMapper(tokenSeparator, propertyNames, checkReoccurringHeader));

    return lines.map(new CreateImportVertexCSV());
  }

  /**
   * Reads the fist row of a csv file and creates a list including all column entries that
   * will be used as property names.
   *
   * @return the property names
   * @throws IOException if an error occurred while open the stream
   */
  private List<String> readHeaderRow() throws IOException {
    try (final BufferedReader reader =
      new BufferedReader(new InputStreamReader(new FileInputStream(path), charset))) {
      String headerLine = reader.readLine();
      if (headerLine == null) {
        throw new IOException("The file " + path + " does not contain any rows.");
      }

      return Arrays.asList(headerLine.split(tokenSeparator));
    } catch (IOException ex) {
      throw new IOException("I/O Error occurred while trying to open a stream to: '" +
        path + "'.", ex);
    }
  }

  /**
   * Reads the input of the csv file as logical graph.
   *
   * @return logical graph of the csv file
   * @throws IOException
   */
  public LogicalGraph getLogicalGraph() throws IOException {

    DataSet<Vertex> importVertices = importVertices();

    return config.getLogicalGraphFactory().fromDataSets(importVertices);
  }
}
