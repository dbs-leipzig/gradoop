/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.examples;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.flink.io.impl.json.JSONDataSink;
import org.gradoop.flink.io.impl.json.JSONDataSource;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;
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
   * File containing EPGM vertices.
   */
  private static final String VERTICES_JSON = "nodes.json";
  /**
   * File containing EPGM edges.
   */
  private static final String EDGES_JSON = "edges.json";
  /**
   * File containing EPGM graph heads.
   */
  private static final String GRAPHS_JSON = "graphs.json";
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
    return new BasicParser().parse(OPTIONS, args);
  }

  /**
   * Reads an EPGM database from a given directory.
   *
   * @param directory path to EPGM database
   * @return EPGM logical graph
   */
  @SuppressWarnings("unchecked")
  protected static LogicalGraph readLogicalGraph(String directory) {
    return readLogicalGraph(directory, true);
  }

  /**
   * Reads an EPGM database from a given directory.
   *
   * @param directory       path to EPGM database
   * @param readGraphHeads  true, if graph heads are contained
   * @return EPGM logical graph
   */
  @SuppressWarnings("unchecked")
  protected static LogicalGraph readLogicalGraph(String directory,
    boolean readGraphHeads) {
    directory = appendSeparator(directory);
    return new JSONDataSource(
      readGraphHeads ? directory + GRAPHS_JSON : null,
      directory + VERTICES_JSON,
      directory + EDGES_JSON,
      GradoopFlinkConfig.createConfig(getExecutionEnvironment()))
      .getLogicalGraph();
  }

  /**
   * Writes a logical graph into a given directory.
   *
   * @param graph     logical graph
   * @param directory output path
   * @throws Exception
   */
  protected static void writeLogicalGraph(LogicalGraph graph, String directory)
      throws Exception {
    directory = appendSeparator(directory);
    graph.writeTo(new JSONDataSink(
      directory + GRAPHS_JSON,
      directory + VERTICES_JSON,
      directory + EDGES_JSON,
      graph.getConfig()));

    getExecutionEnvironment().execute();
  }

  /**
   * Writes a graph collection into a given directory.
   *
   * @param collection  graph collection
   * @param directory   output path
   * @throws Exception
   */
  protected static void writeGraphCollection(GraphCollection collection,
    String directory) throws Exception {
    directory = appendSeparator(directory);
    collection.writeTo(new JSONDataSink(
      directory + GRAPHS_JSON,
      directory + VERTICES_JSON,
      directory + EDGES_JSON,
      collection.getConfig()));

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
  private static String appendSeparator(final String directory) {
    final String fileSeparator = System.getProperty("file.separator");
    String result = directory;
    if (!directory.endsWith(fileSeparator)) {
      result = directory + fileSeparator;
    }
    return result;
  }
}
