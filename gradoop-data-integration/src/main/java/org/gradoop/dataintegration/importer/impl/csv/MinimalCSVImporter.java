/*
 * Copyright © 2014 - 2021 Leipzig University (Database Research Group)
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
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.io.TupleCsvInputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
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
import java.util.stream.IntStream;

/**
 * Read a csv file and import each row as a vertex in EPGM representation.
 */
public class MinimalCSVImporter implements DataSource {

  /**
   * Token delimiter of the CSV file.
   */
  private final String tokenSeparator;

  /**
   * Path to the csv file.
   */
  private final String path;

  /**
   * The charset used in the csv file, e.g., "UTF-8".
   */
  private String charset = "UTF-8";

  /**
   * The property names for all columns in the file. If {@code null}, the first line will be
   * interpreted as header row.
   */
  private final List<String> columnNames;

  /**
   * Flag to specify if each row of the file should be checked for reoccurring of the column property names.
   */
  private boolean checkReoccurringHeader = false;

  /**
   * Flag to specify if quoted strings should be considered. Will escape delimiters inside quoted strings.
   */
  private boolean parseQuotedStrings = false;

  /**
   * Character used to quote strings.
   */
  private char quoteCharacter = '"';

  /**
   * Gradoop Flink configuration.
   */
  private final GradoopFlinkConfig config;

  /**
   * Create a new MinimalCSVImporter instance by the given parameters.
   *
   * @param path the path to the csv file
   * @param tokenSeparator the token delimiter of the csv file
   * @param config GradoopFlinkConfig configuration
   * @param columnNames property identifiers for each column
   */
  public MinimalCSVImporter(String path, String tokenSeparator, GradoopFlinkConfig config,
                            List<String> columnNames) {
    this.path = Objects.requireNonNull(path);
    this.tokenSeparator = Objects.requireNonNull(tokenSeparator);
    this.config = Objects.requireNonNull(config);
    this.columnNames = columnNames;
  }

  /**
   * Create a new MinimalCSVImporter instance by the given parameters. The first line of the file
   * will set as the property names for each column.
   *
   * @param path the path to the csv file
   * @param tokenSeparator the token delimiter of the csv file
   * @param config GradoopFlinkConfig configuration
   */
  public MinimalCSVImporter(String path, String tokenSeparator, GradoopFlinkConfig config) {
    this(path, tokenSeparator, config, null);
  }

  /**
   * Set charset of CSV file. UTF-8 is used as default.
   *
   * @param charset the charset used in the csv file, e.g., "UTF-8"
   * @return this
   */
  public MinimalCSVImporter setCharset(String charset) {
    this.charset = charset;
    return this;
  }

  /**
   * Set checkReoccurringHeader flag.
   * Each row of the file will be checked for reoccurrence of the column property names.
   *
   * @return this
   */
  public MinimalCSVImporter checkReoccurringHeader() {
    this.checkReoccurringHeader = true;
    return this;
  }

  /**
   * Set parseQuotedStrings flag. Delimiters in quoted fields will be escaped.
   *
   * @return this
   */
  public MinimalCSVImporter parseQuotedStrings() {
    this.parseQuotedStrings = true;
    return this;
  }

  /**
   * Set quoteCharacter.
   *
   * @param quoteCharacter character used to quote fields
   * @return this
   */
  public MinimalCSVImporter quoteCharacter(char quoteCharacter) {
    this.quoteCharacter = quoteCharacter;
    return this;
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
    DataSet<EPGMVertex> vertices;

    if (columnNames == null) {
      vertices = readCSVFile(readHeaderRow(), checkReoccurringHeader);
    } else {
      vertices = readCSVFile(columnNames, checkReoccurringHeader);
    }

    return config.getLogicalGraphFactory().fromDataSets(vertices);
  }

  @Override
  public GraphCollection getGraphCollection() throws IOException {
    LogicalGraph logicalGraph = getLogicalGraph();
    return logicalGraph.getCollectionFactory().fromGraph(logicalGraph);
  }

  /**
   * Reads the csv file specified by {@link MinimalCSVImporter#path} and converts each valid line
   * to a {@link EPGMVertex}.
   *
   * @param propertyNames list of the property identifier names
   * @param checkReoccurringHeader set to true if each row of the file should be checked for
   *                               reoccurring of the column property names
   * @return a {@link DataSet} of all vertices from one specific file
   */
  private DataSet<EPGMVertex> readCSVFile(List<String> propertyNames, boolean checkReoccurringHeader) {
    Class<String>[] types = IntStream.range(0, propertyNames.size())
      .mapToObj(i -> String.class)
      .<Class<String>>toArray(Class[]::new);

    return getCSVReader(config.getExecutionEnvironment(), path, types)
      .flatMap(new CsvRowToProperties<>(propertyNames, checkReoccurringHeader))
      .filter(p -> !p.isEmpty())
      .map(new PropertiesToVertex<>(config.getLogicalGraphFactory().getVertexFactory()))
      .returns(config.getLogicalGraphFactory().getVertexFactory().getType());
  }

  /**
   * Create CSVReader for the provided types.
   * Flinks CSVReader builder does not support tuples of dynamic length.
   *
   * @param env execution environment
   * @param filename path of csv file
   * @param fieldTypes array of types
   * @return csv reader returning a tuple of fieldTypes
   */
  private org.apache.flink.api.java.operators.DataSource<Tuple> getCSVReader(ExecutionEnvironment env,
    String filename, Class<?>[] fieldTypes) {
    TupleTypeInfo<Tuple> typeInfo = TupleTypeInfo.getBasicAndBasicValueTupleTypeInfo(fieldTypes);
    TupleCsvInputFormat<Tuple> inputFormat = new TupleCsvInputFormat<>(
      new org.apache.flink.core.fs.Path(filename), typeInfo);
    inputFormat.setCharset(this.charset);
    inputFormat.setFieldDelimiter(this.tokenSeparator);
    inputFormat.setSkipFirstLineAsHeader(false);
    if (this.parseQuotedStrings) {
      inputFormat.enableQuotedStringParsing(this.quoteCharacter);
    }
    return new org.apache.flink.api.java.operators.DataSource<>(env, inputFormat, typeInfo,
      Utils.getCallLocationName());
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
