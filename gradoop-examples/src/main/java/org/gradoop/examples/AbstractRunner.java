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
import org.apache.hadoop.hbase.HBaseConfiguration;
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
import org.gradoop.storage.config.GradoopAccumuloConfig;
import org.gradoop.storage.config.GradoopHBaseConfig;
import org.gradoop.storage.impl.accumulo.AccumuloEPGMStore;
import org.gradoop.storage.impl.accumulo.io.AccumuloDataSink;
import org.gradoop.storage.impl.accumulo.io.AccumuloDataSource;
import org.gradoop.storage.impl.hbase.HBaseEPGMStore;
import org.gradoop.storage.impl.hbase.factory.HBaseEPGMStoreFactory;
import org.gradoop.storage.impl.hbase.io.HBaseDataSink;
import org.gradoop.storage.impl.hbase.io.HBaseDataSource;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Base class for example runners.
 */
public abstract class AbstractRunner {
  /**
   * Command line options for the runner.
   */
  protected static final Options OPTIONS = new Options();
  /**
   * Csv graph source/sink format
   */
  protected static final String FORMAT_CSV = "csv";
  /**
   * (Label) Indexed CSV graph source/sink format
   */
  protected static final String FORMAT_INDEXED = "indexed";
  /**
   * Json graph source/sink format
   */
  protected static final String FORMAT_JSON = "json";
  /**
   * HBase store as graph source/sink format
   */
  protected static final String FORMAT_HBASE = "hbase";
  /**
   * Accumulo store as graph source/sink format
   */
  protected static final String FORMAT_ACCUMULO = "accumulo";
  /**
   * Graph format used as default
   */
  protected static final String DEFAULT_FORMAT = FORMAT_CSV;
  /**
   * Flink execution environment.
   */
  private static ExecutionEnvironment ENV;
  /**
   * HBase store instance
   */
  private static HBaseEPGMStore HBASE_STORE;
  /**
   * Accumulo store instance
   */
  private static AccumuloEPGMStore ACCUMULO_STORE;

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
   * @throws Exception on failure during graph loading
   */
  protected static LogicalGraph readLogicalGraph(String directory) throws Exception {
    return readLogicalGraph(directory, DEFAULT_FORMAT);
  }

  /**
   * Reads an EPGM database from a given directory.
   *
   * @param directory path to EPGM database
   * @param format format in which the graph is stored (csv, indexed, json)
   * @return EPGM logical graph
   * @throws Exception on failure
   */
  protected static LogicalGraph readLogicalGraph(String directory, String format)
    throws Exception {
    return getDataSource(directory, format).getLogicalGraph();
  }

  /**
   * Reads an EPGM database from a given directory.
   *
   * @param directory path to EPGM database
   * @param format format in which the graph collection is stored (csv, indexed, json)
   * @return EPGM graph collection
   * @throws Exception on failure
   */
  protected static GraphCollection readGraphCollection(String directory, String format)
    throws Exception {
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
   * Returns an EPGM DataSource for a given directory and format with a new gradoop flink config.
   *
   * @param directory input path
   * @param format format in which the data is stored (csv, indexed, json)
   * @return DataSource for EPGM Data
   * @throws Exception on failure
   */
  private static DataSource getDataSource(String directory, String format) throws Exception {
    return getDataSource(directory, format,
      GradoopFlinkConfig.createConfig(getExecutionEnvironment()));
  }

  /**
   * Returns an EPGM DataSource for a given directory and format.
   *
   * @param directory input path (or table prefix for store formats, e.g. "mytable.")
   * @param format format in which the data is stored (csv, indexed, json, hbase, accumulo)
   * @param config the gradoop flink configuration
   * @return DataSource for EPGM Data
   * @throws Exception on failure
   */
  protected static DataSource getDataSource(String directory, String format,
    GradoopFlinkConfig config) throws Exception {
    format = format.toLowerCase();

    switch (format) {
    case FORMAT_JSON:
      directory = appendSeparator(directory);
      return new JSONDataSource(directory, config);
    case FORMAT_CSV:
      directory = appendSeparator(directory);
      return new CSVDataSource(directory, config);
    case FORMAT_INDEXED:
      directory = appendSeparator(directory);
      return new IndexedCSVDataSource(directory, config);
    case "lgcsv":
      directory = appendSeparator(directory);
      return new LogicalGraphCSVDataSource(directory, config);
    case "lgindexed":
      directory = appendSeparator(directory);
      return new LogicalGraphIndexedCSVDataSource(directory, config);
    case FORMAT_HBASE:
      return new HBaseDataSource(getHBaseStore(directory), config);
    case FORMAT_ACCUMULO:
      return new AccumuloDataSource(getAccumuloStore(directory), config);
    default:
      throw new IllegalArgumentException("Unsupported format: " + format);
    }
  }

  /**
   * Returns an EPGM DataSink for a given directory and format.
   *
   * @param directory output path (or table prefix for store formats, e.g. "mytable.")
   * @param format output format (csv, indexed, json, hbase, accumulo)
   * @param config gradoop config
   * @return DataSink for EPGM Data
   * @throws Exception if creation of the sink fails
   */
  private static DataSink getDataSink(String directory, String format, GradoopFlinkConfig config)
  throws Exception{
    format = format.toLowerCase();

    switch (format) {
    case FORMAT_JSON:
      directory = appendSeparator(directory);
      return new JSONDataSink(directory, config);
    case FORMAT_CSV:
      directory = appendSeparator(directory);
      return new CSVDataSink(directory, config);
    case FORMAT_INDEXED:
      directory = appendSeparator(directory);
      return new IndexedCSVDataSink(directory, config);
    case FORMAT_HBASE:
      return new HBaseDataSink(getHBaseStore(directory), config);
    case FORMAT_ACCUMULO:
      return new AccumuloDataSink(getAccumuloStore(directory), config);
    default:
      throw new IllegalArgumentException("Unsupported format: " + format);
    }
  }

  /**
   * Get an HBase EPGM store instance that uses the given table prefix.
   *
   * @param prefix the table prefix to use (e.g. "mytable.")
   * @return the store instance
   */
  private static HBaseEPGMStore getHBaseStore(String prefix) {
    if (HBASE_STORE == null) {
      HBASE_STORE = HBaseEPGMStoreFactory
        .createOrOpenEPGMStore(
          HBaseConfiguration.create(),
          GradoopHBaseConfig.getDefaultConfig(),
          prefix);
    }
    return HBASE_STORE;
  }

  /**
   * Get an Accumulo EPGM store instance that uses the given table prefix.
   * It reads properties from file "accumulo.properties" in resource path.
   *
   * @param prefix the table prefix to use (e.g. "mytable.")
   * @return the store instance
   * @throws Exception if creating the store fails
   */
  private static AccumuloEPGMStore getAccumuloStore(String prefix) throws Exception {
    if (ACCUMULO_STORE == null) {
      Properties prop = new Properties();
      String propFileName = "accumulo.properties";

      InputStream inputStream = AbstractRunner.class.getClassLoader()
        .getResourceAsStream(propFileName);

      if (inputStream != null) {
        prop.load(inputStream);
      } else {
        throw new FileNotFoundException("Property file '" + propFileName +
          "' not found in the classpath.");
      }

      if (!prop.containsKey(GradoopAccumuloConfig.ACCUMULO_USER) ||
        !prop.containsKey(GradoopAccumuloConfig.ACCUMULO_PASSWD) ||
        !prop.containsKey(GradoopAccumuloConfig.ACCUMULO_INSTANCE) ||
        !prop.containsKey(GradoopAccumuloConfig.ZOOKEEPER_HOSTS)) {
        throw new IllegalArgumentException("One of the following properties is missing in file '" +
          propFileName + "': '" + GradoopAccumuloConfig.ACCUMULO_USER + "','" +
          GradoopAccumuloConfig.ACCUMULO_PASSWD + "','" +
          GradoopAccumuloConfig.ACCUMULO_INSTANCE + "','" +
          GradoopAccumuloConfig.ZOOKEEPER_HOSTS + "'.");
      }

      // get the property values
      String user = prop.getProperty(GradoopAccumuloConfig.ACCUMULO_USER);
      String password = prop.getProperty(GradoopAccumuloConfig.ACCUMULO_PASSWD);
      String accumuloInstance = prop.getProperty(GradoopAccumuloConfig.ACCUMULO_INSTANCE);
      String zookeeperHosts = prop.getProperty(GradoopAccumuloConfig.ZOOKEEPER_HOSTS);

      ACCUMULO_STORE = new AccumuloEPGMStore(
        GradoopAccumuloConfig.getDefaultConfig()
          .set(GradoopAccumuloConfig.ACCUMULO_TABLE_PREFIX, prefix)
          .set(GradoopAccumuloConfig.ACCUMULO_USER, user)
          .set(GradoopAccumuloConfig.ACCUMULO_INSTANCE, accumuloInstance)
          .set(GradoopAccumuloConfig.ZOOKEEPER_HOSTS, zookeeperHosts)
          .set(GradoopAccumuloConfig.ACCUMULO_PASSWD, password));
    }
    return ACCUMULO_STORE;
  }
}
