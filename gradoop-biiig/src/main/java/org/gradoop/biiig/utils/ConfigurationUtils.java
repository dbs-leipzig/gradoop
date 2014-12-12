package org.gradoop.biiig.utils;

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
  public static final String OPTION_HELP = "h";
  public static final String OPTION_VERBOSE = "v";
  public static final String OPTION_WORKERS = "w";
  public static final String OPTION_REDUCERS = "r";
  public static final String OPTION_HBASE_SCAN_CACHE = "sc";
  public static final String OPTION_DROP_TABLES = "dt";
  public static final String OPTION_GRAPH_INPUT_PATH = "gip";
  public static final String OPTION_GRAPH_OUTPUT_PATH = "gop";

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
    OPTIONS.addOption(OPTION_HBASE_SCAN_CACHE, "scanCache", true, "HBase scan" +
      " cache");
    OPTIONS.addOption(OPTION_DROP_TABLES, "dropTables", false,
      "Drop HBase EPG tables if they exist.");
    OPTIONS.addOption(OPTION_GRAPH_INPUT_PATH, "graphInputPath", true,
      "Graph Input Path");
    OPTIONS.addOption(OPTION_GRAPH_OUTPUT_PATH, "graphOutputPath", true,
      "HFiles output path (used by Bulk Load)");
  }

  public static CommandLine parseArgs(final
  String[] args)
    throws ParseException {
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

  private static void printHelp() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp(ConfigurationUtils.class.getName(), OPTIONS, true);
  }

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
  }
}
