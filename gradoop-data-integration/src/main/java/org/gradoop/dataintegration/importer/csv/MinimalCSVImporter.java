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
package org.gradoop.dataintegration.importer.csv;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Read a csv file and import each row as a vertex in EPGM representation.
 */
public class MinimalCSVImporter {

  /**
   * Log if an error by reading of the file occurs.
   */
  private static final Logger LOG = LoggerFactory.getLogger(MinimalCSVImporter.class);

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
   * Gradoop Flink configuration
   */
  private GradoopFlinkConfig config;

  /**
   * Create a new MinimalCSVImporter. The user set a list of the property names.
   * @param path the path to the csv file
   * @param tokenSeperator the token delimiter of the csv file
   * @param config GradoopFlinkConfig
   * @param columnNames property identifier for each column
   */
  public MinimalCSVImporter(String path, String tokenSeperator, GradoopFlinkConfig config,
          List<String> columnNames) {
    this(path, tokenSeperator, config);
    this.columnNames = columnNames;
  }

  /**
   * Create a new MinimalCSVImporter. Use the utf-8 charset as default. The first line of the file
   * will set as the property names for each column.
   * @param path the path to the csv file
   * @param tokenSeperator the token delimiter of the csv file
   * @param config GradoopFlinkConfig
   */
  public MinimalCSVImporter(String path, String tokenSeperator, GradoopFlinkConfig config) {
    this(path, tokenSeperator, config, "UTF-8");
  }

  /**
   * Create a new MinimalCSVImporter with a user set charset. The first line of the file
   * will set as the property names for each column.
   * @param path the path to the csv file
   * @param tokenSeperator the token delimiter of the csv file
   * @param config GradoopFlinkConfig
   * @param charset the charset used in the csv file
   */
  public MinimalCSVImporter(String path, String tokenSeperator, GradoopFlinkConfig config,
          String charset) {
    this.path = path;
    this.tokenSeparator = tokenSeperator;
    this.config = config;
    this.charset = charset;
  }

  /**
   * Import each row of the file as a vertex. If no column property names are set,
   * read the first line of the file as header and set this values as column names.
   * @param checkReoccurringHeader if each row of the file should be checked for reocurring of
   * the column property names.
   * @return the imported vertices
 * @throws IOException if an error occurred while open the stream
   */
  public DataSet<ImportVertex<Long>> importVertices(boolean checkReoccurringHeader)
          throws IOException {
    if (columnNames == null) {
      return readCSVFile(readHeaderRow(), checkReoccurringHeader);
    } else {
      return readCSVFile(columnNames, checkReoccurringHeader);
    }
  }

  /**
   * Read the vertices from a csv file.
   *
   * @param propertyNames list of the property identifier name
   * @param checkReoccurringHeader if each row of the file should be checked for reocurring of
   * the column property names.
   * @return DateSet of all vertices from one specific file.
   */
  public DataSet<ImportVertex<Long>> readCSVFile(List<String> propertyNames,
          boolean checkReoccurringHeader) {

    DataSet<Properties> lines = config.getExecutionEnvironment()
    .readTextFile(path)
    .map(new RowToVertexMapper(path, tokenSeparator, propertyNames, checkReoccurringHeader))
    .filter(new FilterNullValuesTuple<>());

    return DataSetUtils.zipWithUniqueId(lines).map(new CreateImportVertexCSV<>());
  }

  /**
   * Read the fist row of a csv file and put each the entry in each column in a list.
   * @return the property names
 * @throws IOException if an error occurred while open the stream
   */
  public List<String> readHeaderRow() throws IOException {
    try (final BufferedReader reader =
              new BufferedReader(new InputStreamReader(new FileInputStream(path),
                      charset))) {
      String headerLine = reader.readLine();
      if (headerLine == null) {
        LOG.error("The file do not contain any rows.");
        throw new NullPointerException();
      }
      headerLine = headerLine.substring(0, headerLine.length());
      String[] headerArray;
      headerArray = headerLine.split(tokenSeparator);

      return Arrays.asList(headerArray);
    } catch (IOException ex) {
      LOG.error("I/O Error occurred while trying to open a stream to: '" +
        path + "'.");
      throw new IOException();
    }
  }
}
