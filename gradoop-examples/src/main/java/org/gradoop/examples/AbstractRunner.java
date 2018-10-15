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
package org.gradoop.examples;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.csv.CSVDataSink;
import org.gradoop.flink.io.impl.csv.CSVDataSource;
import org.gradoop.flink.io.impl.csv.indexed.IndexedCSVDataSink;
import org.gradoop.flink.io.impl.csv.indexed.IndexedCSVDataSource;
import org.gradoop.flink.io.impl.deprecated.json.JSONDataSink;
import org.gradoop.flink.io.impl.deprecated.json.JSONDataSource;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.flink.io.impl.deprecated.logicalgraphcsv.LogicalGraphCSVDataSource;
import org.gradoop.flink.io.impl.deprecated.logicalgraphcsv.LogicalGraphIndexedCSVDataSource;

import java.io.File;
import java.io.IOException;

/**
 * Base class for example runners.
 */
public abstract class AbstractRunner {
  /**
   * Command line options for the runner.
   */
  protected static final Options OPTIONS = new Options();
  /**
   * Graph format used as default
   */
  protected static final String DEFAULT_FORMAT = "csv";
  /**
   * Flink execution environment.
   */
  private static ExecutionEnvironment ENV;

  /**
   * Parses the program arguments and performs sanity checks.
   *
   * @param args program arguments
   * @param className executing class name (for help display)
   * @return command line which can be used in the program
   * @throws ParseException on failure
   */
  protected static CommandLine parseArguments(String[] args, String className)
      throws ParseException {
    if (args.length == 0) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(className, OPTIONS, true);
      return null;
    }
    return new DefaultParser().parse(OPTIONS, args);
  }

  /**
   * Reads an EPGM database from a given directory  using a {@link CSVDataSource}.
   *
   * @param directory path to EPGM database
   * @return EPGM logical graph
   */
  protected static LogicalGraph readLogicalGraph(String directory) throws IOException {
    return readLogicalGraph(directory, DEFAULT_FORMAT);
  }

  /**
   * Reads an EPGM database from a given directory.
   *
   * @param directory path to EPGM database
   * @param format format in which the graph is stored (csv, indexed, json)
   * @return EPGM logical graph
   * @throws IOException on failure
   */
  protected static LogicalGraph readLogicalGraph(String directory, String format)
      throws IOException {
    return getDataSource(directory, format).getLogicalGraph();
  }

  /**
   * Reads an EPGM database from a given directory.
   *
   * @param directory path to EPGM database
   * @param format format in which the graph collection is stored (csv, indexed, json)
   * @return EPGM graph collection
   * @throws IOException on failure
   */
  protected static GraphCollection readGraphCollection(String directory, String format)
      throws IOException {
    return getDataSource(directory, format).getGraphCollection();
  }

  /**
   * Writes a logical graph into the specified directory using a {@link CSVDataSink}.
   *
   * @param graph logical graph
   * @param directory output path
   * @throws Exception on failure
   */
  protected static void writeLogicalGraph(LogicalGraph graph, String directory) throws Exception {
    writeLogicalGraph(graph, directory, DEFAULT_FORMAT);
  }

  /**
   * Writes a logical graph into a given directory.
   *
   * @param graph logical graph
   * @param directory output path
   * @param format output format (csv, indexed, json)
   * @throws Exception on failure
   */
  protected static void writeLogicalGraph(LogicalGraph graph, String directory, String format)
      throws Exception {
    graph.writeTo(getDataSink(directory, format, graph.getConfig()), true);
    getExecutionEnvironment().execute();
  }

  /**
   * Writes a graph collection into the specified directory using a {@link CSVDataSink}.
   *
   * @param collection graph collection
   * @param directory output path
   * @throws Exception on failure
   */
  protected static void writeGraphCollection(GraphCollection collection, String directory)
      throws Exception {
    writeGraphCollection(collection, directory, DEFAULT_FORMAT);
  }

  /**
   * Writes a graph collection into a given directory.
   *
   * @param collection graph collection
   * @param directory output path
   * @param format output format (csv, indexed, json)
   * @throws Exception on failure
   */
  protected static void writeGraphCollection(GraphCollection collection,
                                             String directory,
                                             String format)
      throws Exception {
    collection.writeTo(getDataSink(directory, format, collection.getConfig()));
    getExecutionEnvironment().execute();
  }

  /**
   * Returns a Flink execution environment.
   *
   * @return Flink execution environment
   */
  protected static ExecutionEnvironment getExecutionEnvironment() {
    if (ENV == null) {
      ENV = ExecutionEnvironment.getExecutionEnvironment();
    }
    return ENV;
  }

  /**
   * Appends a file separator to the given directory (if not already existing).
   *
   * @param directory directory
   * @return directory with OS specific file separator
   */
  protected static String appendSeparator(final String directory) {
    final String fileSeparator = System.getProperty("file.separator");
    String result = directory;
    if (!directory.endsWith(fileSeparator)) {
      result = directory + fileSeparator;
    }
    return result;
  }

  /**
   * Converts the given DOT file into a PNG image. Note that this method requires the "dot" command
   * to be available locally.
   *
   * @param dotFile path to DOT file
   * @param pngFile path to PNG file
   * @throws IOException on failure
   */
  protected static void convertDotToPNG(String dotFile, String pngFile) throws IOException {
    ProcessBuilder pb = new ProcessBuilder("dot", "-Tpng", dotFile);
    File output = new File(pngFile);
    pb.redirectOutput(ProcessBuilder.Redirect.appendTo(output));
    pb.start();
  }

  /**
   * Returns an EPGM DataSource for a given directory and format.
   *
   * @param directory input path
   * @param format format in which the data is stored (csv, indexed, json)
   * @return DataSource for EPGM Data
   * @throws IOException on failure
   */
  private static DataSource getDataSource(String directory, String format) throws IOException {
    directory = appendSeparator(directory);
    GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(getExecutionEnvironment());
    format = format.toLowerCase();

    switch (format) {
    case "json":
      return new JSONDataSource(directory, config);
    case "csv":
      return new CSVDataSource(directory, config);
    case "indexed":
      return new IndexedCSVDataSource(directory, config);
    case "lgcsv":
      return new LogicalGraphCSVDataSource(directory, config);
    case "lgindexed":
      return new LogicalGraphIndexedCSVDataSource(directory, config);
    default:
      throw new IllegalArgumentException("Unsupported format: " + format);
    }
  }

  /**
   * Returns an EPGM DataSink for a given directory and format.
   *
   * @param directory output path
   * @param format output format (csv, indexed, json)
   * @param config gradoop config
   * @return DataSink for EPGM Data
   */
  private static DataSink getDataSink(String directory, String format, GradoopFlinkConfig config) {
    directory = appendSeparator(directory);
    format = format.toLowerCase();

    switch (format) {
    case "json":
      return new JSONDataSink(directory, config);
    case "csv":
      return new CSVDataSink(directory, config);
    case "indexed":
      return new IndexedCSVDataSink(directory, config);
    default:
      throw new IllegalArgumentException("Unsupported format: " + format);
    }
  }
}
