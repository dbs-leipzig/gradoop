package org.gradoop.utils;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * Translate command line args into Configuration Key-Value pairs.
 */
public class ConfigurationUtils {
  /**
   * Command line option for displaying help.
   */
  public static final String OPTION_HELP = "h";
  /**
   * Command line option for activating verbose.
   */
  public static final String OPTION_VERBOSE = "v";
  /**
   * Command line option to set the number of giraph workers.
   */
  public static final String OPTION_WORKERS = "w";
  /**
   * Command line option to set the number of reducers.
   */
  public static final String OPTION_REDUCERS = "r";
  /**
   * Command line option to set the hbase scan cache value.
   */
  public static final String OPTION_HBASE_SCAN_CACHE = "sc";
  /**
   * Command line option to drop hbase tables if they exist.
   */
  public static final String OPTION_DROP_TABLES = "dt";
  /**
   * Command line option to set the graph file input path.
   */
  public static final String OPTION_GRAPH_INPUT_PATH = "gip";
  /**
   * Command line option to set the path to store HFiles from Bulk Load.
   */
  public static final String OPTION_GRAPH_OUTPUT_PATH = "gop";
  /**
   * Command line option to set the table name for the secondary index.
   */
  public static final String OPTION_SORT_TABLE_NAME = "stn";

  /**
   * Maintains accepted options for gradoop-biiig
   */
  private static Options OPTIONS;

  static {
    OPTIONS = new Options();
    OPTIONS.addOption(OPTION_HELP, "help", false, "Help");
    OPTIONS.addOption(OPTION_VERBOSE, "verbose", false, "Print output");
    OPTIONS
      .addOption(OPTION_WORKERS, "workers", true, "Number of giraph workers");
    OPTIONS.addOption(OPTION_REDUCERS, "reducers", true, "Number of reducers");
    OPTIONS.addOption(OPTION_HBASE_SCAN_CACHE, "scanCache", true,
      "HBase scan" + " cache");
    OPTIONS.addOption(OPTION_DROP_TABLES, "dropTables", false,
      "Drop HBase EPG tables if they exist.");
    OPTIONS.addOption(OPTION_GRAPH_INPUT_PATH, "graphInputPath", true,
      "Graph Input Path");
    OPTIONS.addOption(OPTION_GRAPH_OUTPUT_PATH, "graphOutputPath", true,
      "HFiles output path (used by Bulk Load)");
    OPTIONS.addOption(OPTION_SORT_TABLE_NAME, "sortTableName", true, "HTable " +
      "used to store secondary index.");
  }

  /**
   * Parses the given arguments.
   *
   * @param args command line arguments
   * @return parsed command line
   * @throws ParseException
   */
  public static CommandLine parseArgs(final String[] args) throws
    ParseException {
    if (args.length == 0) {
      throw new IllegalArgumentException("No arguments were provided (try -h)");
    }
    CommandLineParser parser = new BasicParser();
    CommandLine cmd = parser.parse(OPTIONS, args);

    if (cmd.hasOption(OPTION_HELP)) {
      printHelp();
      return null;
    }
    performSanityCheck(cmd);

    return cmd;
  }

  /**
   * Prints a help menu for the defined options.
   */
  private static void printHelp() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp(ConfigurationUtils.class.getName(), OPTIONS, true);
  }

  /**
   * Checks if the given arguments make sense.
   *
   * @param cmd command line
   */
  private static void performSanityCheck(final CommandLine cmd) {
    if (!cmd.hasOption(OPTION_WORKERS)) {
      throw new IllegalArgumentException("Chose the number of workers (-w)");
    }
    if (!cmd.hasOption(OPTION_GRAPH_INPUT_PATH)) {
      throw new IllegalArgumentException("Chose the graph input path (-gip");
    }
    if (!cmd.hasOption(OPTION_GRAPH_OUTPUT_PATH)) {
      throw new IllegalArgumentException("Chose the graph output path (-gop");
    }
    if (!cmd.hasOption(OPTION_SORT_TABLE_NAME)) {
      throw new IllegalArgumentException("Chose the sort table name (-stn");
    }
  }
}
