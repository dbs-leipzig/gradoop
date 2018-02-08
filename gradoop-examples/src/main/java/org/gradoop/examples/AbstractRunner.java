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
package org.gradoop.examples;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.flink.io.impl.csv.CSVDataSink;
import org.gradoop.flink.io.impl.csv.CSVDataSource;
import org.gradoop.flink.io.impl.json.JSONDataSink;
import org.gradoop.flink.io.impl.json.JSONDataSource;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * Base class for example runners.
 */
public abstract class AbstractRunner {
  /**
   * Command line options for the runner.
   */
  protected static final Options OPTIONS = new Options();
  /**
   * Flink execution environment.
   */
  private static ExecutionEnvironment ENV;

  /**
   * Parses the program arguments and performs sanity checks.
   *
   * @param args      program arguments
   * @param className executing class name (for help display)
   * @return command line which can be used in the program
   * @throws ParseException
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
   * Reads an EPGM database from a given directory.
   *
   * @param directory path to EPGM database
   * @return EPGM logical graph
   */
  @SuppressWarnings("unchecked")
  protected static LogicalGraph readLogicalGraph(String directory) {
    return readLogicalGraph(directory, "json");
  }

  /**
   * Reads an EPGM database from a given directory.
   *
   * @param directory path to EPGM database
   * @param format    format in which the graph is stored (csv, json)
   * @return EPGM logical graph
   */
  @SuppressWarnings("unchecked")
  protected static LogicalGraph readLogicalGraph(String directory, String format) {
    directory = appendSeparator(directory);

    GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(getExecutionEnvironment());
    format = format.toLowerCase();

    if (format.equals("json")) {
      return new JSONDataSource(directory, config).getLogicalGraph();
    } else if (format.equals("csv")) {
      return new CSVDataSource(directory, config).getLogicalGraph();
    } else {
      throw new IllegalArgumentException("Unsupported format: " + format);
    }
  }

  /**
   * Writes a logical graph into the specified directory using a {@link JSONDataSink}.
   *
   * @param graph     logical graph
   * @param directory output path
   * @throws Exception
   */
  protected static void writeLogicalGraph(LogicalGraph graph, String directory) throws Exception {
    writeLogicalGraph(graph, directory, "json");
  }

  /**
   * Writes a logical graph into a given directory.
   *
   * @param graph     logical graph
   * @param directory output path
   * @param format output format (json, csv)
   * @throws Exception
   */
  protected static void writeLogicalGraph(LogicalGraph graph, String directory, String format)
    throws Exception {

    format = format.toLowerCase();
    if (format.equals("json")) {
      graph.writeTo(new JSONDataSink(appendSeparator(directory), graph.getConfig()));
    } else if (format.equals("csv")) {
      graph.writeTo(new CSVDataSink(appendSeparator(directory), graph.getConfig()));
    }
    getExecutionEnvironment().execute();
  }

  /**
   * Writes a graph collection into the specified directory using a {@link JSONDataSink}.
   *
   * @param collection  graph collection
   * @param directory   output path
   * @throws Exception
   */
  protected static void writeGraphCollection(GraphCollection collection, String directory)
    throws Exception {
    collection.writeTo(new JSONDataSink(appendSeparator(directory), collection.getConfig()));
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
}
