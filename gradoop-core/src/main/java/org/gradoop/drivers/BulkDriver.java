///*
// * This file is part of Gradoop.
// *
// * Gradoop is free software: you can redistribute it and/or modify
// * it under the terms of the GNU General Public License as published by
// * the Free Software Foundation, either version 3 of the License, or
// * (at your option) any later version.
// *
// * Gradoop is distributed in the hope that it will be useful,
// * but WITHOUT ANY WARRANTY; without even the implied warranty of
// * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// * GNU General Public License for more details.
// *
// * You should have received a copy of the GNU General Public License
// * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
// */
//
//package org.gradoop.drivers;
//
//import com.google.common.base.Splitter;
//import com.google.common.collect.Iterables;
//import org.apache.commons.cli.BasicParser;
//import org.apache.commons.cli.CommandLine;
//import org.apache.commons.cli.CommandLineParser;
//import org.apache.commons.cli.HelpFormatter;
//import org.apache.commons.cli.Options;
//import org.apache.commons.cli.ParseException;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.conf.Configured;
//import org.apache.hadoop.util.Tool;
//import org.apache.log4j.Logger;
//import org.gradoop.GConstants;
//
///**
// * BulkDriver SuperClass for all Driver
// */
//public abstract class BulkDriver extends Configured implements Tool {
//  /**
//   * Command line option for displaying help.
//   */
//  public static final String OPTION_HELP = "h";
//  /**
//   * Command line option for activating verbose.
//   */
//  public static final String OPTION_VERBOSE = "v";
//  /**
//   * Command line option to set the graph file input path.
//   */
//  public static final String OPTION_GRAPH_INPUT_PATH = "gip";
//  /**
//   * Command line option to set the path to store HFiles from Bulk Load.
//   */
//  public static final String OPTION_GRAPH_OUTPUT_PATH = "gop";
//  /**
//   * Create custom vertices table for different use cases.
//   */
//  public static final String OPTION_TABLE_PREFIX = "tp";
//  /**
//   * Command line option to set a custom argument.
//   */
//  public static final String OPTION_CUSTOM_ARGUMENT = "ca";
//  /**
//   * Class Logger
//   */
//  private static Logger LOG = Logger.getLogger(BulkDriver.class);
//  /**
//   * Verbose var for bulkload and buldwrite driver
//   */
//  private boolean verbose;
//  /**
//   * graph input path
//   */
//  private String inputPath;
//  /**
//   * vertices table name
//   */
//  private String verticesTableName;
//  /**
//   * graphs table name
//   */
//  private String graphsTableName;
//  /**
//   * graph output path
//   */
//  private String outputPath;
//  /**
//   * Giraph configuration
//   */
//  private Configuration conf;
//
//  /**
//   * Get Method for verbose var
//   *
//   * @return verbose
//   */
//  public boolean getVerbose() {
//    return this.verbose;
//  }
//
//  /**
//   * Get Method for input path
//   *
//   * @return inputPath
//   */
//  public String getInputPath() {
//    return this.inputPath;
//  }
//
//  /**
//   * Get Method for output path
//   *
//   * @return outputpath
//   */
//  public String getOutputPath() {
//    return this.outputPath;
//  }
//
//  /**
//   * Get vertices table name
//   *
//   * @return outputpath
//   */
//  public String getVerticesTableName() {
//    return this.verticesTableName;
//  }
//
//  /**
//   * Get vertices table name
//   *
//   * @return outputpath
//   */
//  public String getGraphsTableName() {
//    return this.graphsTableName;
//  }
//
//  /**
//   * Get Method for giraph Configuration
//   *
//   * @return conf
//   */
//  public Configuration getHadoopConf() {
//    return this.conf;
//  }
//
//  /**
//   * Method to parse the given arguments
//   *
//   * @param args arguments of the command line
//   * @return int check number
//   * @throws ParseException
//   */
//  public int parseArgs(String[] args) throws ParseException {
//    LOG.info("inner parseArgs");
//    conf = getConf();
//    CommandLine cmd = ConfUtils.parseArgs(args);
//    if (cmd.hasOption(OPTION_HELP)) {
//      printHelp();
//      return 0;
//    }
//    verbose = cmd.hasOption(OPTION_VERBOSE);
//    inputPath = cmd.getOptionValue(OPTION_GRAPH_INPUT_PATH);
//    outputPath = cmd.getOptionValue(OPTION_GRAPH_OUTPUT_PATH);
//    String tablePrefix =
//      cmd.getOptionValue(OPTION_TABLE_PREFIX, "");
//    if (tablePrefix.isEmpty()) {
//      verticesTableName = GConstants.DEFAULT_TABLE_VERTICES;
//      graphsTableName = GConstants.DEFAULT_TABLE_GRAPHS;
//    } else {
//      verticesTableName = tablePrefix + GConstants.DEFAULT_TABLE_VERTICES;
//      graphsTableName = tablePrefix + GConstants.DEFAULT_TABLE_GRAPHS;
//    }
//    if (cmd.hasOption(OPTION_CUSTOM_ARGUMENT)) {
//      for (String caOptionValue : cmd.getOptionValues(OPTION_CUSTOM_ARGUMENT)) {
//        for (String paramValue : Splitter.on(',').split(caOptionValue)) {
//          String[] parts =
//            Iterables.toArray(Splitter.on('=').split(paramValue), String.class);
//          if (parts.length != 2) {
//            throw new IllegalArgumentException("Unable to parse custom " +
//              " argument: " + paramValue);
//          }
//          parts[1] = parts[1].replaceAll("\"", "");
//          if (LOG.isInfoEnabled()) {
//            LOG.info("###Setting custom argument [" + parts[0] + "] to [" +
//              parts[1] + "] in HadoopConfiguration");
//          }
//          conf.set(parts[0], parts[1]);
//        }
//      }
//    }
//    return 1;
//  }
//
//  /**
//   * Prints a help menu for the defined options.
//   */
//  private static void printHelp() {
//    HelpFormatter formatter = new HelpFormatter();
//    formatter.printHelp(ConfUtils.class.getName(), ConfUtils.OPTIONS, true);
//  }
//
//  /**
//   * Configuration params for {@link org.gradoop.drivers.BulkLoadDriver}.
//   */
//  public static class ConfUtils {
//    /**
//     * Maintains accepted options for {@link org.gradoop.drivers.BulkLoadDriver}
//     */
//    protected static final Options OPTIONS;
//
//    static {
//      OPTIONS = new Options();
//      OPTIONS.addOption(OPTION_HELP, "help", false, "Display help.");
//      OPTIONS.addOption(OPTION_VERBOSE, "verbose", false,
//        "Print console output during job execution.");
//      OPTIONS.addOption(OPTION_GRAPH_INPUT_PATH, "graph-input-path", true,
//        "Path to the input graph file.");
//      OPTIONS.addOption(OPTION_GRAPH_OUTPUT_PATH, "graph-output-path", true,
//        "Path to store HFiles in HDFS (used by Bulk Load)");
//      OPTIONS.addOption(OPTION_TABLE_PREFIX, "table-vertices-prefix",
//        true, "Custom prefix for vertices table to distinguish different use " +
//          "cases.");
//      OPTIONS.addOption(OPTION_CUSTOM_ARGUMENT, "customArguments", true,
//        "provide custom" +
//          " arguments for the job configuration in the form:" +
//          " -ca <param1>=<value1>,<param2>=<value2> -ca <param3>=<value3> etc" +
//          "." +
//          " It can appear multiple times, and the last one has effect" +
//          " for the same param.");
//    }
//
//    /**
//     * Parses the given arguments.
//     *
//     * @param args command line arguments
//     * @return parsed command line
//     * @throws org.apache.commons.cli.ParseException
//     */
//    public static CommandLine parseArgs(final String[] args) throws
//      ParseException {
//      if (args.length == 0) {
//        LOG.error("No arguments were provided (try -h)");
//      }
//      CommandLineParser parser = new BasicParser();
//      return parser.parse(OPTIONS, args, false);
//    }
//  }
//}
